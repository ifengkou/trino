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

import com.zaxxer.hikari.HikariDataSource;
import io.airlift.log.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * JDBC base functions
 * @author shenlongguang https://github.com/ifengkou
 * @date: 2021/7/14
 */
public abstract class BaseJdbcService
{
    private static final Logger log = Logger.get(BaseJdbcService.class);
    public HikariDataSource dataSource;
    public boolean enable;

    /**
     * 获取 Connection
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
            throws Exception
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
            throw e;
        }
        finally {
            if (state != null) {
                try {
                    state.close();
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
        return rs;
    }

    /**
     * 执行查询SQL语句
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
            if (rs != null) {
                try {
                    rs.close();
                }
                catch (SQLException e) {
                    log.error(e, e.getMessage());
                }
            }

            if (state != null) {
                try {
                    state.close();
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
            if (rs != null) {
                try {
                    rs.close();
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
            if (state != null) {
                try {
                    state.close();
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
        return success;
    }

    /**
     * callback interface
     */
    public interface QueryCallback
    {
        void process(ResultSet rs)
                throws Exception;
    }
}
