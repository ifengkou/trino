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

import io.airlift.configuration.Config;

/**
 * @author shenlongguang https://github.com/ifengkou
 * @date: 2021/7/26
 */
public class MysqlConfig
{
    private boolean enable;
    private String driverClassName;
    private String jdbcUrl;
    private String userName;
    private String password;
    private int maxPoolSize;

    public boolean isEnable()
    {
        return enable;
    }

    @Config("trino.extension.mysql.enable")
    public MysqlConfig setEnable(boolean enable)
    {
        this.enable = enable;
        return this;
    }

    public String getDriverClassName()
    {
        return driverClassName;
    }

    @Config("trino.extension.mysql.driver")
    public MysqlConfig setDriverClassName(String driverClassName)
    {
        this.driverClassName = driverClassName;
        return this;
    }

    public String getJdbcUrl()
    {
        return jdbcUrl;
    }

    @Config("trino.extension.mysql.url")
    public MysqlConfig setJdbcUrl(String jdbcUrl)
    {
        this.jdbcUrl = jdbcUrl;
        return this;
    }

    public String getUserName()
    {
        return userName;
    }

    @Config("trino.extension.mysql.username")
    public MysqlConfig setUserName(String userName)
    {
        this.userName = userName;
        return this;
    }

    public String getPassword()
    {
        return password;
    }

    @Config("trino.extension.mysql.password")
    public MysqlConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    public int getMaxPoolSize()
    {
        return maxPoolSize;
    }

    @Config("trino.extension.mysql.max-pool-size")
    public MysqlConfig setMaxPoolSize(int maxPoolSize)
    {
        this.maxPoolSize = maxPoolSize;
        return this;
    }
}
