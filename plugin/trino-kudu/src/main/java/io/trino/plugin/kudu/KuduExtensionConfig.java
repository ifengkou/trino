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

import io.airlift.configuration.Config;

/**
 * The kudu extension service configuration
 *
 * @author shenlongguang https://github.com/ifengkou
 * @date: 2021/8/9
 */
public class KuduExtensionConfig
{
    private boolean isArrayEnable;
    private int decimalDefaultPrecision = 18;
    private int decimalDefaultScale = 3;
    /**
     * the schema prefix that  supports array data-type
     * not all schema need to support array data-type
     */
    private String schemaPrefix;
    /**
     * the table prefix that  supports array data-type
     * not all schema need to support array data-type
     */
    private String tablePrefix;

    public boolean isArrayEnable()
    {
        return isArrayEnable;
    }

    @Config("kudu.extension.array.enable")
    public KuduExtensionConfig setArrayEnable(boolean arrayEnable)
    {
        isArrayEnable = arrayEnable;
        return this;
    }

    public int getDecimalDefaultPrecision()
    {
        return decimalDefaultPrecision;
    }

    @Config("kudu.extension.decimal.precision")
    public KuduExtensionConfig setDecimalDefaultPrecision(int decimalDefaultPrecision)
    {
        this.decimalDefaultPrecision = decimalDefaultPrecision;
        return this;
    }

    public int getDecimalDefaultScale()
    {
        return decimalDefaultScale;
    }

    @Config("kudu.extension.decimal.scale")
    public KuduExtensionConfig setDecimalDefaultScale(int decimalDefaultScale)
    {
        this.decimalDefaultScale = decimalDefaultScale;
        return this;
    }

    public String getSchemaPrefix()
    {
        return schemaPrefix;
    }

    @Config("kudu.extension.array.schema.prefix")
    public KuduExtensionConfig setSchemaPrefix(String schemaPrefix)
    {
        this.schemaPrefix = schemaPrefix;
        return this;
    }

    public String getTablePrefix()
    {
        return tablePrefix;
    }

    @Config("kudu.extension.array.table.prefix")
    public KuduExtensionConfig setTablePrefix(String tablePrefix)
    {
        this.tablePrefix = tablePrefix;
        return this;
    }
}
