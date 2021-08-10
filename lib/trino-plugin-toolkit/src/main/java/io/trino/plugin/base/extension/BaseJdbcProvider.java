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
package io.trino.plugin.base.extension;

import com.google.inject.Inject;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.airlift.log.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * JDBC base functions
 *
 * @author shenlongguang https://github.com/ifengkou
 * @date: 2021/7/14
 */
public class BaseJdbcProvider
        implements JdbcProvider
{
    private static final Logger log = Logger.get(BaseJdbcProvider.class);

    private JdbcConfig config;

    private HikariDataSource dataSource;
    public boolean enable;

    @Inject
    public BaseJdbcProvider(JdbcConfig jdbcConfig)
    {
        this.config = jdbcConfig;
        if (this.config.isEnable()) {
            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setDriverClassName(config.getDriverClassName());
            hikariConfig.setJdbcUrl(config.getJdbcUrl());
            hikariConfig.setUsername(config.getUserName());
            hikariConfig.setPassword(config.getPassword());
            hikariConfig.setMaximumPoolSize(config.getMaxPoolSize());
            if (config.getJdbcUrl().contains("mysql")) {
                hikariConfig.addDataSourceProperty("cachePrepStmts", "true");
                hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
                hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
                hikariConfig.addDataSourceProperty("useServerPrepStmts", true);
            }
            this.dataSource = new HikariDataSource(hikariConfig);
            this.enable = true;
            log.info("--- Trino Extension: Add BaseJdbcProvider ---");
        }
        else {
            this.dataSource = null;
            this.enable = false;
        }
    }

    public boolean isEnable()
    {
        return this.enable == true && this.dataSource != null;
    }

    /**
     * get Connection
     *
     * @return
     */
    public Connection getConnection()
    {
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
        }
        catch (SQLException e) {
            log.error(e, e.getMessage());
        }
        return conn;
    }

    public int executeUpdate(String sql)
    {
        Connection conn = null;
        Statement state = null;
        int rs = 0;

        try {
            conn = getConnection();
            state = conn.createStatement();
            rs = state.executeUpdate(sql);
        }
        catch (Exception e) {
            log.error(e, e.getMessage());
            throw new RuntimeException("executeUpdate error", e);
        }
        finally {
            closeJdbcSession(null, state, null, conn);
        }
        return rs;
    }

    /**
     * executeQuery
     *
     * @param sql sql
     * @param callback callback
     */
    public void executeQuery(String sql, QueryCallback callback)
    {
        Connection conn = null;
        Statement state = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            state = conn.createStatement();
            rs = state.executeQuery(sql);
            callback.process(rs);
        }
        catch (Exception e) {
            log.error(e, e.getMessage());
            throw new RuntimeException("executeQuery error", e);
        }
        finally {
            closeJdbcSession(rs, state, null, conn);
        }
    }

    /**
     * Execute Query with PrepareStatement
     *
     * @param sql
     * @param data
     * @param callback
     */
    public void executeQuery(String sql, Object[] data, QueryCallback callback)
    {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            ps = conn.prepareStatement(sql);
            for (int i = 0; i < data.length; i++) {
                ps.setObject(i + 1, data[i]);
            }
            rs = ps.executeQuery();
            callback.process(rs);
        }
        catch (Exception e) {
            log.error(e, e.getMessage());
            throw new RuntimeException("executeQuery error", e);
        }
        finally {
            closeJdbcSession(rs, null, ps, conn);
        }
    }

    public boolean execute(String sql)
    {
        Connection conn = null;
        Statement state = null;
        boolean success = false;

        try {
            conn = getConnection();
            state = conn.createStatement();
            state.execute(sql);
            success = true;
        }
        catch (Exception e) {
            log.error(e, e.getMessage());
        }
        finally {
            closeJdbcSession(null, state, null, conn);
        }
        return success;
    }

    public boolean execute(String sql, Object[] data)
    {
        Connection conn = null;
        PreparedStatement ps = null;
        boolean success = false;
        try {
            conn = getConnection();
            ps = conn.prepareStatement(sql);
            for (int i = 0; i < data.length; i++) {
                ps.setObject(i + 1, data[i]);
            }
            ps.execute();
            success = true;
        }
        catch (SQLException e) {
            log.error(e, e.getMessage());
        }
        finally {
            closeJdbcSession(null, null, ps, conn);
        }
        return success;
    }

    public void streamQuery(String sql, int fetchSize, StreamQueryCallback callback)
    {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            connection = this.getConnection();
            ps = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            ps.setFetchSize(fetchSize);
            rs = ps.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            callback.process(rsmd, rs);
        }
        catch (SQLException e) {
            log.error(e.getMessage());
        }
        catch (IOException e) {
            log.error(e.getMessage());
        }
        catch (Exception e) {
            log.error(e, e.getMessage());
        }
        finally {
            closeJdbcSession(rs, null, ps, connection);
        }
    }

    private void closeJdbcSession(ResultSet rs, Statement st, PreparedStatement ps, Connection conn)
    {
        if (rs != null) {
            try {
                rs.close();
            }
            catch (SQLException e) {
                log.error(e, e.getMessage());
            }
        }

        if (st != null) {
            try {
                st.close();
            }
            catch (SQLException e) {
                log.error(e, e.getMessage());
            }
        }

        if (ps != null) {
            try {
                ps.close();
            }
            catch (SQLException e) {
                log.error(e, e.getMessage());
            }
        }

        if (conn != null) {
            try {
                conn.close();
            }
            catch (SQLException e) {
                log.error(e, e.getMessage());
            }
        }
    }
}
