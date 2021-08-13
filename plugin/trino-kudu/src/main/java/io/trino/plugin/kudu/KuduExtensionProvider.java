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
package io.trino.plugin.kudu;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.spi.extension.JdbcProvider;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.sql.ResultSet;

/**
 * The kudu extension service provider
 *
 * 1. Support two array data type: array<string>,array<double>
 * 2. Not use NULL to overwrite the original value when inserting part of the column
 *
 * @author shenlongguang https://github.com/ifengkou
 * @date: 2021/8/4
 */
public class KuduExtensionProvider
{
    private static final Logger log = Logger.get(KuduExtensionProvider.class);

    public static final int DECIMAL_DEFAULT_PRECISION = 18;
    public static final int DECIMAL_DEFAULT_SCALE = 3;
    public static final String DATATYPE_NUMBER = "number";
    public static final String DATATYPE_ARRAY_STRING = "array<string>";
    public static final String ARRAY_STRING_SPLITTER = "\001";
    public static final String ARRAY_DECIMAL_COLUMN_SQL = "select code, data_type from %s.td_base_attr_info where type='%s' and data_type in('array<string>', 'array<double>', 'number') ORDER BY id asc;";

    private final JdbcProvider jdbcProvider;
    private final KuduExtensionConfig kuduExtensionConfig;

    @Inject
    public KuduExtensionProvider(JdbcProvider jdbcProvider, KuduExtensionConfig config)
    {
        log.info("--- kudu extension: Init KuduExtensionProvider");
        this.jdbcProvider = jdbcProvider;
        this.kuduExtensionConfig = config;
    }

    /**
     * Gets columns of types array and decimal from db
     *
     * @param schemaName
     * @param tableName
     * @return
     */
    public ImmutableMap<String, Type> loadSpecialColumns(String schemaName, String tableName)
    {
        log.info("--kudu extension: loadSpecialColumns");
        ImmutableMap.Builder<String, Type> specialColsMap = new ImmutableMap.Builder<>();
        if (!kuduExtensionConfig.isArrayEnable()) {
            return specialColsMap.build();
        }
        if (!isSupportedArray(schemaName, kuduExtensionConfig.getSchemaPrefix())) {
            return specialColsMap.build();
        }

        if (!isSupportedArray(tableName, kuduExtensionConfig.getTablePrefix())) {
            return specialColsMap.build();
        }
        String tablePrefix = getPrefix(tableName, kuduExtensionConfig.getTablePrefix());
        // The database in DB and the schema in kudu must have the same name
        String sql = String.format(ARRAY_DECIMAL_COLUMN_SQL, schemaName, tablePrefix);
        log.info("--loadSpecialColumns: sql=" + sql);
        try {
            jdbcProvider.executeQuery(sql,
                    ((ResultSet rs) -> {
                        while (rs.next()) {
                            String fieldName = rs.getString(1);
                            String fieldType = rs.getString(2);
                            Type prestoType;
                            if (DATATYPE_NUMBER.equals(fieldType)) {
                                prestoType = DecimalType.createDecimalType(DECIMAL_DEFAULT_PRECISION, DECIMAL_DEFAULT_SCALE);
                            }
                            else if (DATATYPE_ARRAY_STRING.equals(fieldType)) {
                                prestoType = new ArrayType(VarcharType.VARCHAR);
                                log.info("-- " + fieldName + ":array<string>");
                            }
                            else {
                                prestoType = new ArrayType(DoubleType.DOUBLE);
                                log.info("-- " + fieldName + ":array<double>");
                            }
                            specialColsMap.put(fieldName, prestoType);
                        }
                    }));
        }
        catch (Exception e) {
            log.error("--loadSpecialColumns failed: Cannot get the meta info from db. NOT BREAK");
        }

        return specialColsMap.build();
    }

    private static boolean isSupportedArray(String origin, String prefixStr)
    {
        if (origin == null || "".equals(origin) || prefixStr == null || "".equals(prefixStr)) {
            return false;
        }
        String[] prefixArray = prefixStr.split(",");
        for (String prefix : prefixArray) {
            if (origin.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private static String getPrefix(String origin, String prefixStr)
    {
        String[] prefixArray = prefixStr.split(",");
        for (String prefix : prefixArray) {
            if (origin.startsWith(prefix)) {
                return prefix;
            }
        }
        return null;
    }
}
