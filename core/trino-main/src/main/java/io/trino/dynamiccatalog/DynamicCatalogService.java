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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import io.airlift.http.client.HttpStatus;
import io.airlift.log.Logger;
import io.trino.spi.extension.JdbcProvider;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * dynamic catalog service
 *
 * @author shenlongguang https://github.com/ifengkou
 * @date: 2021/7/21
 */
public class DynamicCatalogService
{
    private static final Logger log = Logger.get(DynamicCatalogService.class);

    private static final MediaType MEDIA_TYPE_JSON = MediaType.parse("application/json; charset=utf-8");

    private final JdbcProvider jdbcProvider;
    public boolean enable;
    // load from properties?
    private static final String CATALOG_QUERY_SQL = "select catalogName,connectorName,properties from %s";
    private static final String CATALOG_INSERT_SQL = "insert into %s(catalogName,connectorName,properties) values(?,?,?)";
    private static final String CATALOG_DELETE_SQL = "delete from %s where catalogName=?";
    private static final String CATALOG_CHECK_SQL = "select catalogName from %s where catalogName=?";

    private String querySql;
    private String insertSql;
    private String deleteSql;
    private String checkSql;

    OkHttpClient okHttpClient = new OkHttpClient.Builder()
            .callTimeout(30, TimeUnit.SECONDS)
            .build();

    @Inject
    public DynamicCatalogService(DynamicCatalogStoreConfig config, JdbcProvider jdbcProvider)
    {
        log.info("-- DynamicCatalogService,config.enable=" + config.isEnable() + ",jdbcProvider.enable=" + jdbcProvider.isEnable());
        this.enable = config.isEnable() && jdbcProvider.isEnable();
        String tableName = config.getTableName();
        querySql = String.format(CATALOG_QUERY_SQL, tableName);
        insertSql = String.format(CATALOG_INSERT_SQL, tableName);
        deleteSql = String.format(CATALOG_DELETE_SQL, tableName);
        checkSql = String.format(CATALOG_CHECK_SQL, tableName);
        this.jdbcProvider = jdbcProvider;
    }

    /**
     * Load catalogs from db
     *
     * @return
     */
    public List<Map<String, String>> loadCatalogsFromDb()
    {
        if (!this.jdbcProvider.isEnable()) {
            throw new RuntimeException("-- dynamic catalog need 'extension.mysql' is enable");
        }
        List<Map<String, String>> catalogVos = new ArrayList<>(16);
        this.jdbcProvider.executeQuery(this.querySql, ((ResultSet rs) -> {
            while (rs.next()) {
                String catalogName = rs.getString("catalogName");
                String connectorName = rs.getString("connectorName");
                String properties = rs.getString("properties");
                if (!Strings.isNullOrEmpty(catalogName) && !Strings.isNullOrEmpty(connectorName)) {
                    Map<String, String> catalogMap = new HashMap<>(3);
                    catalogMap.put("catalogName", catalogName);
                    catalogMap.put("connectorName", connectorName);
                    catalogMap.put("properties", properties);
                    catalogVos.add(catalogMap);
                }
            }
        }));
        return catalogVos;
    }

    /**
     * Check whether the same catalogName already exists in the db
     *
     * @param catalogVo
     * @return
     */
    public boolean checkCatalogExistedInDb(CatalogVo catalogVo)
    {
        requireNonNull(catalogVo.getCatalogName(), "catalogName is null");
        requireNonNull(catalogVo.getConnectorName(), "connectorName is null");
        Object[] params = new Object[] {catalogVo.catalogName};
        List<String> savedCatalog = new ArrayList<>(8);
        this.jdbcProvider.executeQuery(this.checkSql, params, ((ResultSet rs) -> {
            while (rs.next()) {
                String catalogName = rs.getString("catalogName");
                savedCatalog.add(catalogName);
            }
        }));
        return savedCatalog.size() > 0;
    }

    /**
     * Save catalog to db
     *
     * @param catalogVo
     * @return
     */
    public boolean addCatalogToDb(CatalogVo catalogVo)
    {
        if (!this.jdbcProvider.isEnable()) {
            throw new RuntimeException("dynamic catalog need 'extension.mysql' is enable");
        }
        boolean isExisted = checkCatalogExistedInDb(catalogVo);
        if (isExisted) {
            log.error("-- Catalog " + catalogVo.getCatalogName() + " existed");
            return false;
        }
        String properties = null;
        if (catalogVo.getProperties() != null) {
            ObjectMapper mapper = new ObjectMapper();
            try {
                properties = mapper.writeValueAsString(catalogVo.getProperties());
            }
            catch (JsonProcessingException e) {
                properties = null;
            }
        }
        properties = "".equals(properties) ? null : properties;
        Object[] params = new Object[] {catalogVo.catalogName, catalogVo.connectorName, properties};
        return this.jdbcProvider.execute(this.insertSql, params);
    }

    /**
     * Delete catalog from db
     *
     * @param catalogName
     * @return
     */
    public boolean deleteCatalogFromDb(String catalogName)
    {
        if (!this.jdbcProvider.isEnable()) {
            throw new RuntimeException("dynamic catalog need 'extension.mysql' is enable");
        }
        Object[] params = new Object[] {catalogName};
        return this.jdbcProvider.execute(this.deleteSql, params);
    }

    public boolean noticeWorkerAddCatalog(URI workerUri, CatalogVo catalogVo)
    {
        ObjectMapper objectMapper = new ObjectMapper();
        String requestBody = null;
        try {
            requestBody = objectMapper.writeValueAsString(catalogVo);
        }
        catch (JsonProcessingException e) {
            log.error("-- noticeWorkerAddCatalog: Invalid catalogVo: " + e.getMessage());
            return false;
        }
        HttpUrl url = HttpUrl.get(workerUri);
        if (url == null) {
            log.error("-- noticeWorkerAddCatalog: Invalid worker URL:" + workerUri);
            return false;
        }
        url = url.newBuilder().encodedPath("/v1/catalog/add").build();
        Request.Builder builder = new Request.Builder().url(url)
                .post(RequestBody.create(MEDIA_TYPE_JSON, requestBody));
        try (Response response = okHttpClient.newCall(builder.build()).execute()) {
            log.info(response.protocol() + " " + response.code() + " " + response.message());
            if (response.code() == HttpStatus.OK.code()) {
                String responseBody = response.body().string();
                ResponseParser responseParser = objectMapper.readValue(responseBody, ResponseParser.class);
                if (responseParser != null) {
                    if (responseParser.getStatusCode() == 200) {
                        log.info("-- noticeWorkerAddCatalog: Successfully added catalog with " + workerUri);
                        return true;
                    }
                    else {
                        log.error("-- noticeWorkerAddCatalog: Failed add catalog: " + responseBody);
                    }
                }
            }
        }
        catch (IOException e) {
            log.error("-- noticeWorkerAddCatalog: Failed Call addCatalog API:" + e.getMessage());
        }

        return false;
    }

    public boolean noticeWorkerDeleteCatalog(URI workerUri, String catalogName)
    {
        HttpUrl url = HttpUrl.get(workerUri);
        if (url == null) {
            log.error("Invalid worker URL:" + workerUri);
            return false;
        }
        url = url.newBuilder().encodedPath("/v1/catalog/delete")
                .addQueryParameter("catalogName", catalogName).build();
        log.info("-- noticeWorkerAddCatalog: request url is %s", url.toString());
        Request.Builder builder = new Request.Builder().url(url).delete();
        try (Response response = okHttpClient.newCall(builder.build()).execute()) {
            ObjectMapper objectMapper = new ObjectMapper();
            if (response.code() == HttpStatus.OK.code()) {
                String responseBody = response.body().string();
                ResponseParser responseParser = objectMapper.readValue(responseBody, ResponseParser.class);
                if (responseParser != null && responseParser.getStatusCode() == 200) {
                    if (responseParser.getStatusCode() == 200) {
                        log.info("-- noticeWorkerAddCatalog: Successfully deleted catalog:{} ,with " + workerUri);
                        return true;
                    }
                    else {
                        log.error("-- noticeWorkerAddCatalog: Failed delete catalog: " + responseBody);
                    }
                }
            }
        }
        catch (IOException e) {
            log.error("-- noticeWorkerAddCatalog: Failed Call deleteCatalog API:" + e.getMessage());
        }

        return false;
    }
}
