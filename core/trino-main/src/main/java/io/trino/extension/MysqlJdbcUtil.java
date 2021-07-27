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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.airlift.log.Logger;

/**
 * Recommended use MysqlJdbcService
 *
 * @author shenlongguang https://github.com/ifengkou
 * @date: 2021/7/14
 */
public class MysqlJdbcUtil
        extends BaseJdbcService
{
    private static final Logger log = Logger.get(MysqlJdbcUtil.class);

    private static MysqlJdbcUtil instance;

    public static final MysqlJdbcUtil getInstance(HikariConfig hikariConfig)
    {
        if (instance == null) {
            synchronized (MysqlJdbcUtil.class) {
                if (instance == null) {
                    instance = new MysqlJdbcUtil(hikariConfig);
                }
            }
        }
        return instance;
    }

    private MysqlJdbcUtil(HikariConfig hikariConfig)
    {
        try {
            dataSource = new HikariDataSource(hikariConfig);
            enable = true;
        }
        catch (Exception e) {
            log.error(e, e.getMessage());
        }
    }
}
