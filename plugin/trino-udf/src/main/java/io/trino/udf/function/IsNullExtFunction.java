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
package io.trino.udf.function;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.StandardTypes;

/**
 * @author shenlongguang https://github.com/ifengkou
 * @date: 2021/11/5
 */
@Description("Returns TRUE if the argument is NULL")
@ScalarFunction("isnull")
public final class IsNullExtFunction
{
    private IsNullExtFunction()
    {
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BIGINT)
    public static long isNullOrEmpty(@SqlNullable @SqlType("T") Slice value)
    {
        return (value == null || value.length() == 0) ? 1 : 0;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BIGINT)
    public static long isNullLong(@SqlNullable @SqlType("T") Long value)
    {
        return (value == null) ? 1 : 0;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BIGINT)
    public static long isNullDouble(@SqlNullable @SqlType("T") Double value)
    {
        return (value == null) ? 1 : 0;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BIGINT)
    public static long isNullBoolean(@SqlNullable @SqlType("T") Boolean value)
    {
        return (value == null) ? 1 : 0;
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.BIGINT)
    public static long isNullBlock(@SqlNullable @SqlType("T") Block value)
    {
        return (value == null) ? 1 : 0;
    }
}
