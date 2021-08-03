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

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.testing.Closeables;
import io.trino.client.ProtocolHeaders;
import io.trino.server.testing.TestingTrinoServer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.trino.testing.assertions.Assert.assertEquals;

/**
 * @author shenlongguang https://github.com/ifengkou
 * @date: 2021/7/5
 */
@Test(singleThreaded = true)
public class TestDynamicCatalogController
{
    private static final JsonCodec<CatalogVo> catalogVoJsonCodec = JsonCodec.jsonCodec(CatalogVo.class);
    private TestingTrinoServer server;
    private HttpClient client;
    private static final Logger log = Logger.get(TestDynamicCatalogController.class);

    @BeforeMethod
    public void setup()
            throws Exception
    {
        server = TestingTrinoServer.create();
        client = new JettyHttpClient();
        cleanUpCatalog();
    }

    @SuppressWarnings("deprecation")
    @AfterMethod
    public void teardown()
    {
        cleanUpCatalog();
        //Closeables.closeQuietly(server);
        //Closeables.closeQuietly(client);
        try {
            Closeables.closeAll(server, client);
        }
        catch (IOException e) {
            log.error("Close server or client failed");
        }
    }

    @Test
    public void testAddCatalog()
    {
        CatalogVo catalogVo = getFakeCatalogObject("system", "testing");
        assertEquals(executeAddCatalogCall(catalogVo).getStatusCode(), 200);
    }

    @Test
    public void testAddCatalogFailed()
    {
        CatalogVo catalogVo = getFakeCatalogObject("invalidConnector", "invalidCatalog");
        assertEquals(executeAddCatalogCall(catalogVo).getStatusCode(), 500);
    }

    @Test
    public void testDeleteCatalog()
    {
        testAddCatalog();
        assertEquals(deleteCatalog().getStatusCode(), 200);
    }

    @Test
    public void testDeleteCatalogFailed()
    {
        String catalogName = getFakeCatalogObject("invalidConnector", "invalidCatalog").getCatalogName();
        testAddCatalog();
        Request request = prepareDelete().setHeader(ProtocolHeaders.TRINO_HEADERS.requestUser(), "user")
                .setUri(uriForDelete(catalogName))
                .build();
        StatusResponseHandler.StatusResponse response = client.execute(request, createStatusResponseHandler());
        assertEquals(response.getStatusCode(), 500);
    }

    private URI uriFor(String path)
    {
        return uriBuilderFrom(server.getBaseUrl()).replacePath(path).build();
    }

    private URI uriForDelete(String catalogName)
    {
        return uriBuilderFrom(server.getBaseUrl()).replacePath("/v1/catalog/delete").addParameter("catalogName", catalogName).build();
    }

    private CatalogVo getFakeCatalogObject(String connectorName, String catalogName)
    {
        CatalogVo catalogVo = new CatalogVo();
        catalogVo.setCatalogName(catalogName);
        catalogVo.setConnectorName(connectorName);
        Map<String, String> map = new HashMap<>();
        map.put("connector.name", connectorName);
        map.put("connection-url", "jdbc:postgresql://localhost:5432/postgres");
        map.put("connection-user", "postgres");
        map.put("connection-password", "postgres");
        catalogVo.setProperties(map);
        return catalogVo;
    }

    private void cleanUpCatalog()
    {
        if (deleteCatalog().getStatusCode() == 200) {
            log.debug("TestDynamicCatalogController:cleanUpCatalog() Successfully deleted catalog");
        }
        else {
            log.debug("TestDynamicCatalogController:cleanUpCatalog() Not able to deleted catalog");
        }
    }

    private StatusResponseHandler.StatusResponse deleteCatalog()
    {
        String catalogName = getFakeCatalogObject("system", "testing").getCatalogName();
        Request request = prepareDelete().setHeader(ProtocolHeaders.TRINO_HEADERS.requestUser(), "user")
                .setUri(uriForDelete(catalogName))
                .build();
        StatusResponseHandler.StatusResponse response = client.execute(request, createStatusResponseHandler());
        return response;
        //return deletePropertyFile(catalogName);
    }

    private boolean deletePropertyFile(String catalogName)
    {
        return new File(getPropertyFilePath(catalogName)).delete();
    }

    private String getPropertyFilePath(String catalogName)
    {
        File catalogConfigurationDir = new File("etc/catalog/");
        return catalogConfigurationDir.getPath() + File.separator + catalogName + ".properties";
    }

    private StatusResponseHandler.StatusResponse executeAddCatalogCall(CatalogVo catalogVo)
    {
        Request request = preparePost().setHeader(ProtocolHeaders.TRINO_HEADERS.requestUser(), "user")
                .setUri(uriFor("/v1/catalog/add"))
                .setBodyGenerator(jsonBodyGenerator(catalogVoJsonCodec, catalogVo))
                .setHeader(ProtocolHeaders.TRINO_HEADERS.requestSource(), "source")
                .setHeader(ProtocolHeaders.TRINO_HEADERS.requestCatalog(), "catalog")
                .setHeader(ProtocolHeaders.TRINO_HEADERS.requestSchema(), "schema")
                .setHeader(ProtocolHeaders.TRINO_HEADERS.requestTimeZone(), "invalid time zone")
                .setHeader("Content-Type", "application/json")
                .build();
        StatusResponseHandler.StatusResponse response = client.execute(
                request,
                createStatusResponseHandler());
        return response;
    }
}
