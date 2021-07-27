/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.dynamiccatalog;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.inject.Inject;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.log.Logger;
import io.trino.connector.CatalogName;
import io.trino.connector.ConnectorManager;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.NodeState;
import io.trino.server.security.ResourceSecurity;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.net.URI;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.nullToEmpty;
import static io.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;

/**
 * https://github.com/prestodb/presto/pull/12605
 *
 * @author shenlongguang https://github.com/ifengkou
 * @date: 2021/7/5
 */
@Path("/v1/catalog")
public class DynamicCatalogController
{
    private final CatalogManager catalogManager;
    private final ConnectorManager connectorManager;
    private final Announcer announcer;
    private final InternalNodeManager internalNodeManager;
    private static final Logger log = Logger.get(DynamicCatalogController.class);
    private final ResponseParser responseParser;
    private final DynamicCatalogService dynamicCatalogService;
    //keep alive,fixed number,queue
    Executor executor = Executors.newFixedThreadPool(3);

    @Inject
    public DynamicCatalogController(CatalogManager catalogManager, ConnectorManager connectorManager, Announcer announcer, InternalNodeManager internalNodeManager, ResponseParser responseParser, DynamicCatalogService service)
    {
        this.catalogManager = catalogManager;
        this.connectorManager = connectorManager;
        this.announcer = announcer;
        this.internalNodeManager = internalNodeManager;
        this.responseParser = responseParser;
        this.dynamicCatalogService = service;
    }

    @Path("add")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ResourceSecurity(PUBLIC)
    public Response addCatalog(CatalogVo catalogVo)
    {
        try {
            if (!this.dynamicCatalogService.enable) {
                return failedResponse(responseParser.build("DynamicCatalog is not enabled", 500));
            }
            log.info("addCatalog() : Input values are " + catalogVo);
            InternalNode currentNode = this.internalNodeManager.getCurrentNode();

            // add catalog and updateConnectorIdAnnouncement
            CatalogName catalogName = createCatalog(catalogVo.getCatalogName(), catalogVo.getConnectorName(), catalogVo.getProperties());
            log.info("addCatalog() : Successfully added catalog " + catalogName.getCatalogName());

            // save to db,then announce all worker
            if (currentNode.isCoordinator()) {
                log.info("addCatalog() : execute coordinator logic:save to db,then announce all worker");
                // Coordinator logic
                // add to  mysql catalog meta table
                boolean saved = this.dynamicCatalogService.addCatalogToDb(catalogVo);
                if (!saved) {
                    log.error("addCatalog() ： Error saving catalog to db.");
                    //not breaking;
                }
                // find all node
                Set<InternalNode> activeNodes = this.internalNodeManager.getNodes(NodeState.ACTIVE);
                if (activeNodes != null && activeNodes.size() > 0) {
                    activeNodes = activeNodes.stream().filter(n -> !n.isCoordinator()).collect(Collectors.toSet());
                    // announce all worker
                    CountDownLatch latch = new CountDownLatch(activeNodes.size());
                    Map<URI, Boolean> noticeResult = new HashMap<>(activeNodes.size());
                    for (InternalNode node : activeNodes) {
                        log.info("addCatalog: Announce node=%s,uri=%s", node.getNodeIdentifier(), node.getInternalUri());
                        executor.execute(() -> {
                            boolean noticeSuccess = dynamicCatalogService.noticeWorkerAddCatalog(node.getInternalUri(), catalogVo);
                            noticeResult.put(node.getInternalUri(), noticeSuccess);
                            latch.countDown();
                        });
                    }
                    latch.await();
                    //TODO Exception handle: rollback?
                    // Try again
                    noticeResult.keySet().stream().forEach((k) -> {
                        if (!noticeResult.get(k)) {
                            log.info("addCatalog: Announce node again,uri=" + k.getHost());
                            dynamicCatalogService.noticeWorkerAddCatalog(k, catalogVo);
                        }
                    });
                }
            }

            // Response
            return successResponse(responseParser.build("Successfully added catalog: " + catalogVo.getCatalogName(), 200));
        }
        catch (Exception ex) {
            log.error("addCatalog() : Error adding catalog " + ex.getMessage());
            return failedResponse(responseParser.build("Error adding Catalog: " + ex.getMessage(), 500));
        }
    }

    @Path("delete")
    @DELETE
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ResourceSecurity(PUBLIC)
    public Response deleteCatalog(@QueryParam("catalogName") String catalogName)
    {
        if (!this.dynamicCatalogService.enable) {
            return failedResponse(responseParser.build("DynamicCatalog is not enabled", 500));
        }
        log.info("deleteCatalog : catalogName = " + catalogName);
        InternalNode currentNode = this.internalNodeManager.getCurrentNode();
        if (currentNode.isCoordinator()) {
            // Coordinator logic
            log.info("deleteCatalog : execute coordinator logic: save to db,then announce all worker");
            // delete to  mysql catalog meta table
            this.dynamicCatalogService.deleteCatalogFromDb(catalogName);
            // find all node
            Set<InternalNode> activeNodes = this.internalNodeManager.getNodes(NodeState.ACTIVE);
            if (activeNodes != null && activeNodes.size() > 0) {
                activeNodes = activeNodes.stream().filter(n -> !n.isCoordinator()).collect(Collectors.toSet());
                // announce all worker
                CountDownLatch latch = new CountDownLatch(activeNodes.size());
                Map<URI, Boolean> noticeResult = new HashMap<>(activeNodes.size());
                for (InternalNode node : activeNodes) {
                    log.info("deleteCatalog: announce node=%s,uri=%s", node.getNodeIdentifier(), node.getInternalUri());
                    executor.execute(() -> {
                        boolean noticeSuccess = dynamicCatalogService.noticeWorkerDeleteCatalog(node.getInternalUri(), catalogName);
                        noticeResult.put(node.getInternalUri(), noticeSuccess);
                        latch.countDown();
                    });
                }
                try {
                    latch.await();
                }
                catch (InterruptedException e) {
                    log.error("deleteCatalog: " + e.getMessage());
                }
                //TODO Exception handle: rollback
                noticeResult.keySet().stream().forEach((k) -> {
                    if (!noticeResult.get(k)) {
                        log.info("deleteCatalog: Announce node again,uri=" + k.getHost());
                        dynamicCatalogService.noticeWorkerDeleteCatalog(k, catalogName);
                    }
                });
            }
        }
        log.info("deleteCatalog(): Deleting catalog: " + catalogName);
        String responseMessage = "";
        if (catalogManager.getCatalog(catalogName).isPresent()) {
            log.info("deleteCatalog() : Catalog exists so deleting catalog " + catalogName);
            connectorManager.dropConnection(catalogName);
            responseMessage = "Successfully deleted";
        }
        else {
            log.info("deleteCatalog() : Catalog doesn't exists, Can't be deleted " + catalogName);
            return failedResponse(responseParser.build("Catalog doesn't exists: " + catalogName, 500));
        }
        log.info("deleteCatalog() : successfully deleted catalog " + catalogName);
        return successResponse(responseParser.build(responseMessage + " : " + catalogName, 200));
    }

    private CatalogName createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        CatalogName catalog = connectorManager.createCatalog(catalogName, connectorName, properties);
        updateConnectorIdAnnouncement(announcer, catalog, internalNodeManager);
        return catalog;
    }

    private static void updateConnectorIdAnnouncement(Announcer announcer, CatalogName catalogName, InternalNodeManager nodeManager)
    {
        //
        // This code was copied from TrinoServer, and is a hack that should be removed when the connectorId property is removed
        //

        // get existing announcement
        ServiceAnnouncement announcement = getTrinoAnnouncement(announcer.getServiceAnnouncements());

        // update connectorIds property
        Map<String, String> properties = new LinkedHashMap<>(announcement.getProperties());
        String property = nullToEmpty(properties.get("connectorIds"));
        Set<String> connectorIds = new LinkedHashSet<>(Splitter.on(',').trimResults().omitEmptyStrings().splitToList(property));
        connectorIds.add(catalogName.toString());
        properties.put("connectorIds", Joiner.on(',').join(connectorIds));

        // update announcement
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(serviceAnnouncement(announcement.getType()).addProperties(properties).build());
        announcer.forceAnnounce();

        nodeManager.refreshNodes();
    }

    private static ServiceAnnouncement getTrinoAnnouncement(Set<ServiceAnnouncement> announcements)
    {
        for (ServiceAnnouncement announcement : announcements) {
            if (announcement.getType().equals("trino")) {
                return announcement;
            }
        }
        throw new RuntimeException("Trino announcement not found: " + announcements);
    }

    private Response successResponse(ResponseParser responseParser)
    {
        return Response.status(Response.Status.OK).entity(responseParser).type(MediaType.APPLICATION_JSON).build();
    }

    private Response failedResponse(ResponseParser responseParser)
    {
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(responseParser).type(MediaType.APPLICATION_JSON).build();
    }
}
