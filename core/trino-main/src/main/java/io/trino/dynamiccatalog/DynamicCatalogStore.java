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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.connector.ConnectorManager;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;

/**
 * load catalog from mysql table
 *
 * @author shenlongguang https://github.com/ifengkou
 * @date: 2021/7/14
 */
public class DynamicCatalogStore
{
    private static final Logger log = Logger.get(DynamicCatalogStore.class);
    private final ConnectorManager connectorManager;
    private final AtomicBoolean catalogsLoading = new AtomicBoolean();
    private DynamicCatalogService dynamicCatalogService;

    @Inject
    public DynamicCatalogStore(ConnectorManager connectorManager, DynamicCatalogService service)
    {
        this.connectorManager = connectorManager;
        this.dynamicCatalogService = service;
    }

    public void loadCatalogs()
            throws Exception
    {
        if (!catalogsLoading.compareAndSet(false, true)) {
            return;
        }
        log.info("-- Loading catalog from db --");
        List<Map<String, String>> catalogVos = this.dynamicCatalogService.loadCatalogsFromDb();
        log.info("-- Start load catalog, total %d catalog--", catalogVos.size());
        for (Map<String, String> catalogMap : catalogVos) {
            loadCatalog(catalogMap);
        }
    }

    private void loadCatalog(Map<String, String> catalogMap)
            throws Exception
    {
        String catalogName = catalogMap.get("catalogName");
        String connectorName = catalogMap.get("connectorName");
        checkState(catalogName != null, "Catalog configuration does not contain catalogName");
        checkState(connectorName != null, "Catalog configuration %s does not contain connectorName", catalogName);

        log.info("-- Loading catalog %s --", catalogName);
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> properties = mapper.readValue(catalogMap.get("properties"), Map.class);
        Map<String, String> newProperties = new HashMap<>();
        if (properties != null && properties.size() > 0) {
            for (String key : properties.keySet()) {
                if (properties.get(key) != null) {
                    newProperties.put(key, properties.get(key).toString());
                }
            }
        }

        connectorManager.createCatalog(catalogName, connectorName, ImmutableMap.copyOf(newProperties));
        log.info("-- Added catalog %s using connector %s --", catalogName, connectorName);
    }
}
