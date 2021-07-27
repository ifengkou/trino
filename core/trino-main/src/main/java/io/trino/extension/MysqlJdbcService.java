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
package io.trino.extension;

import com.google.inject.Inject;
import com.mysql.jdbc.Driver;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.airlift.log.Logger;

/**
 * @author shenlongguang https://github.com/ifengkou
 * @date: 2021/7/14
 */
public class MysqlJdbcService
        extends BaseJdbcService
{
    private static final Logger log = Logger.get(MysqlJdbcService.class);

    private MysqlConfig config;

    @Inject
    public MysqlJdbcService(MysqlConfig mysqlConfig)
    {
        this.config = mysqlConfig;
        if (this.config.isEnable()) {
            HikariConfig hikariConfig = new HikariConfig();
            if (config.getDriverClassName() == null) {
                hikariConfig.setDriverClassName(Driver.class.getName());
            }
            else {
                hikariConfig.setDriverClassName(config.getDriverClassName());
            }

            hikariConfig.setJdbcUrl(config.getJdbcUrl());
            hikariConfig.setUsername(config.getUserName());
            hikariConfig.setPassword(config.getPassword());
            hikariConfig.setMaximumPoolSize(config.getMaxPoolSize());
            hikariConfig.addDataSourceProperty("cachePrepStmts", "true");
            hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
            hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
            hikariConfig.addDataSourceProperty("useServerPrepStmts", true);
            this.dataSource = new HikariDataSource(hikariConfig);
            this.enable = true;
            log.info("Trino Extension: Add MySQLJDBCService ---");
        }
        else {
            this.dataSource = null;
            this.enable = false;
        }
    }
}
