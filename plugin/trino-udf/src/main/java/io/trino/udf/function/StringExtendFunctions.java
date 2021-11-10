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

import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.SliceUtf8.offsetOfCodePoint;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.Math.abs;

/**
 * string function extends
 * @author shenlongguang https://github.com/ifengkou
 * @date: 2021/10/30
 */
public final class StringExtendFunctions
{
    private StringExtendFunctions() {}

    @Description("Returns index of first occurrence of a substring (or 0 if not found)")
    @ScalarFunction("find")
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.BIGINT)
    public static long find(@SqlType("varchar(x)") Slice substring, @SqlType("varchar(y)") Slice string)
    {
        return find(substring, string, 1);
    }

    @Description("Returns index of first occurrence of a substring (or 0 if not found)")
    @ScalarFunction("find")
    @LiteralParameters({"x", "y"})
    @SqlType(StandardTypes.BIGINT)
    public static long find(@SqlType("varchar(x)") Slice substring, @SqlType("varchar(y)") Slice string, @SqlType(StandardTypes.BIGINT) long start)
    {
        if (start < 0) {
            return stringPositionFromEnd(string, substring, abs(start));
        }
        return stringPositionFromStart(string, substring, start);
    }

    @Description("Substring starting at first char")
    @ScalarFunction("leftx")
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice left(@SqlType("varchar(x)") Slice utf8)
    {
        return left(utf8, 1);
    }

    @Description("Substring of given length starting at first char")
    @ScalarFunction("leftx")
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice left(@SqlType("varchar(x)") Slice utf8, @SqlType(StandardTypes.BIGINT) long length)
    {
        if (length <= 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "'length' must be a positive number.");
        }
        return substring(utf8, 1, length);
    }

    @Description("Substring starting at last char")
    @ScalarFunction("rightx")
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice right(@SqlType("varchar(x)") Slice utf8)
    {
        return right(utf8, 1);
    }

    @Description("Substring of given length starting at last char")
    @ScalarFunction("rightx")
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice right(@SqlType("varchar(x)") Slice utf8, @SqlType(StandardTypes.BIGINT) long length)
    {
        if (length <= 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "'length' must be a positive number.");
        }
        if (length > utf8.length()) {
            length = utf8.length();
        }
        return substring(utf8, -length, length);
    }

    @Description("Substring of given length starting at mid index")
    @ScalarFunction("mid")
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice mid(@SqlType("varchar(x)") Slice utf8, @SqlType(StandardTypes.BIGINT) long start, @SqlType(StandardTypes.BIGINT) long length)
    {
        if (length <= 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "'length' must be a positive number.");
        }
        if ((start + length) > utf8.length()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "'start'+'length' must less the text's length.");
        }
        return substring(utf8, start, length);
    }

    private static Slice substring(Slice utf8, long start, long length)
    {
        if (start == 0 || (length <= 0) || (utf8.length() == 0)) {
            return Slices.EMPTY_SLICE;
        }

        int startCodePoint = Ints.saturatedCast(start);
        int lengthCodePoints = Ints.saturatedCast(length);

        if (startCodePoint > 0) {
            int indexStart = offsetOfCodePoint(utf8, startCodePoint - 1);
            if (indexStart < 0) {
                // before beginning of string
                return Slices.EMPTY_SLICE;
            }
            int indexEnd = offsetOfCodePoint(utf8, indexStart, lengthCodePoints);
            if (indexEnd < 0) {
                // after end of string
                indexEnd = utf8.length();
            }

            return utf8.slice(indexStart, indexEnd - indexStart);
        }

        // negative start is relative to end of string
        int codePoints = countCodePoints(utf8);
        startCodePoint += codePoints;

        // before beginning of string
        if (startCodePoint < 0) {
            return Slices.EMPTY_SLICE;
        }

        int indexStart = offsetOfCodePoint(utf8, startCodePoint);
        int indexEnd;
        if (startCodePoint + lengthCodePoints < codePoints) {
            indexEnd = offsetOfCodePoint(utf8, indexStart, lengthCodePoints);
        }
        else {
            indexEnd = utf8.length();
        }

        return utf8.slice(indexStart, indexEnd - indexStart);
    }

    private static long stringPositionFromStart(Slice string, Slice substring, long instance)
    {
        if (instance <= 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "'instance' must be a positive or negative number.");
        }
        if (substring.length() == 0) {
            return 1;
        }

        int foundInstances = 0;
        int index = -1;
        do {
            // step forwards through string
            index = string.indexOf(substring, index + 1);
            if (index < 0) {
                return 0;
            }
            foundInstances++;
        }
        while (foundInstances < instance);

        return countCodePoints(string, 0, index) + 1;
    }

    private static long stringPositionFromEnd(Slice string, Slice substring, long instance)
    {
        if (instance <= 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "'instance' must be a positive or negative number.");
        }
        if (substring.length() == 0) {
            return 1;
        }

        int foundInstances = 0;
        int index = string.length();
        do {
            // step backwards through string
            index = string.toStringUtf8().lastIndexOf(substring.toStringUtf8(), index - 1);
            if (index < 0) {
                return 0;
            }
            foundInstances++;
        }
        while (foundInstances < instance);

        return index + 1;
    }
}
