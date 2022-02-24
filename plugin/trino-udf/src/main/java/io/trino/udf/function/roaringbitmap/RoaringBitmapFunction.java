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
package io.trino.udf.function.roaringbitmap;

import io.airlift.slice.Slice;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.Base64;

/**
 * @author shenlongguang https://github.com/ifengkou
 * @date: 2021/12/8
 */
public final class RoaringBitmapFunction
{
    private RoaringBitmapFunction()
    {
    }

    @ScalarFunction("rbm_size")
    @SqlType(StandardTypes.BIGINT)
    public static long getRoaringBitMapCardinality(
            @SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        byte[] bitmapData = slice.getBytes();
        Roaring64NavigableMap rbm64 = RoaringBitMapUtils.deserialize64(bitmapData);
        return rbm64.getLongCardinality();
    }

    @ScalarFunction("rbm_size")
    @SqlType(StandardTypes.BIGINT)
    public static long getRoaringBitMapStrCardinality(
            @SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        String base64Rbm = slice.toStringUtf8();
        byte[] bitmapData = Base64.getDecoder().decode(base64Rbm);
        Roaring64NavigableMap rbm64 = RoaringBitMapUtils.deserialize64(bitmapData);
        return rbm64.getLongCardinality();
    }

    @ScalarFunction("bitmapContains")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean roaring64BitMapContains(
            @SqlType(StandardTypes.VARBINARY) Slice slice, @SqlType(StandardTypes.BIGINT) long element)
    {
        byte[] bitmapData = slice.getBytes();
        Roaring64NavigableMap rbm64 = RoaringBitMapUtils.deserialize64(bitmapData);
        return rbm64.contains(element);
    }

    @ScalarFunction("bitmapContains")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean roaring64BitMapStrContains(
            @SqlType(StandardTypes.VARCHAR) Slice slice, @SqlType(StandardTypes.BIGINT) long element)
    {
        String base64Rbm = slice.toStringUtf8();
        byte[] bitmapData = Base64.getDecoder().decode(base64Rbm);
        Roaring64NavigableMap rbm64 = RoaringBitMapUtils.deserialize64(bitmapData);
        return rbm64.contains(element);
    }
}
