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

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.joda.time.Chronology;
import org.joda.time.Period;
import org.joda.time.chrono.ISOChronology;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;

/**
 * from http://git.tingyun.com/gejun/presto-ext
 *
 * @author gejun
 * @date: 2021/10/12
 */
public final class TimeFunctions
{
    private TimeFunctions()
    {
    }

    private static final Logger log = Logger.get(TimeFunctions.class);
    //private static final Chronology DEFAULT_CHRONOLOGY = ISOChronology.getInstance(DateTimeZone.forOffsetHours(+8));
    //private static final long origin = new DateTime(0, DateTimeZone.UTC).withZoneRetainFields(DEFAULT_CHRONOLOGY.getZone()).getMillis()
    private static final Chronology DEFAULT_CHRONOLOGY = ISOChronology.getInstance();
    private static final long origin = 0;

    @Description("truncate with nm/h/d/w/M/y")
    @ScalarFunction("time_trunc")
    @LiteralParameters("x")
    @SqlType(StandardTypes.DATE)
    public static long truncateDate(@SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.DATE) long date)
    {
        /*date = scaleEpochMicrosToMillis(date);
        long result = truncate(unit.toStringUtf8(), date);
        return scaleEpochMillisToMicros(result);*/
        return truncate(unit.toStringUtf8(), date);
    }

    @Description("truncate with nm/h/d/w/M/y")
    @ScalarFunction("time_trunc")
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIME)
    public static long truncateTime(@SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.TIME) long time)
    {
        /*time = scaleEpochMicrosToMillis(time);
        long result = truncate(unit.toStringUtf8(), time);
        return scaleEpochMillisToMicros(result);*/
        return truncate(unit.toStringUtf8(), time);
    }

    @Description("truncate with nm/h/d/w/M/y")
    @ScalarFunction("time_trunc")
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long truncateTimestamp(@SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        log.info("=====truncate timestamp = " + timestamp + ",unit=" + unit.toStringUtf8());
        timestamp = scaleEpochMicrosToMillis(timestamp);
        long result = truncate(unit.toStringUtf8(), timestamp);
        result = scaleEpochMillisToMicros(result);
        log.info("=====truncate result = " + result);
        return result;
    }

    public static long scaleEpochMicrosToMillis(long value)
    {
        return Math.floorDiv(value, MICROSECONDS_PER_MILLISECOND);
    }

    public static long scaleEpochMillisToMicros(long epochMillis)
    {
        return Math.multiplyExact(epochMillis, MICROSECONDS_PER_MILLISECOND);
    }

    private static long truncate(String granularity, long t)
    {
        int factor = Integer.parseInt(granularity.substring(0, granularity.length() - 1));
        String unit = granularity.substring(granularity.length() - 1);
        Period period = period(factor, unit);
        final int years = period.getYears();
        if (years > 0) {
            if (years > 1) {
                int y = DEFAULT_CHRONOLOGY.years().getDifference(t, origin);
                y -= y % years;
                long tt = DEFAULT_CHRONOLOGY.years().add(origin, y);
                // always round down to the previous period (for timestamps prior to origin)
                if (t < tt) {
                    t = DEFAULT_CHRONOLOGY.years().add(tt, -years);
                }
                else {
                    t = tt;
                }
                return t;
            }
            else {
                return DEFAULT_CHRONOLOGY.year().roundFloor(t);
            }
        }

        final int months = period.getMonths();
        if (months > 0) {
            if (months > 1) {
                int m = DEFAULT_CHRONOLOGY.months().getDifference(t, origin);
                m -= m % months;
                long tt = DEFAULT_CHRONOLOGY.months().add(origin, m);
                // always round down to the previous period (for timestamps prior to origin)
                if (t < tt) {
                    t = DEFAULT_CHRONOLOGY.months().add(tt, -months);
                }
                else {
                    t = tt;
                }
                return t;
            }
            else {
                return DEFAULT_CHRONOLOGY.monthOfYear().roundFloor(t);
            }
        }

        final int weeks = period.getWeeks();
        if (weeks > 0) {
            if (weeks > 1) {
                // align on multiples from origin
                int w = DEFAULT_CHRONOLOGY.weeks().getDifference(t, origin);
                w -= w % weeks;
                long tt = DEFAULT_CHRONOLOGY.weeks().add(origin, w);
                // always round down to the previous period (for timestamps prior to origin)
                if (t < tt) {
                    t = DEFAULT_CHRONOLOGY.weeks().add(tt, -weeks);
                }
                else {
                    t = tt;
                }
                return t;
            }
            else {
                t = DEFAULT_CHRONOLOGY.dayOfWeek().roundFloor(t);
                // default to Monday as beginning of the week
                return DEFAULT_CHRONOLOGY.dayOfWeek().set(t, 1);
            }
        }

        final int days = period.getDays();
        if (days > 0) {
            if (days > 1) {
                // align on multiples from origin
                int d = DEFAULT_CHRONOLOGY.days().getDifference(t, origin);
                d -= d % days;
                long tt = DEFAULT_CHRONOLOGY.days().add(origin, d);
                // always round down to the previous period (for timestamps prior to origin)
                if (t < tt) {
                    t = DEFAULT_CHRONOLOGY.days().add(tt, -days);
                }
                else {
                    t = tt;
                }
                return t;
            }
            else {
                t = DEFAULT_CHRONOLOGY.hourOfDay().roundFloor(t);
                return DEFAULT_CHRONOLOGY.hourOfDay().set(t, 0);
            }
        }

        final int hours = period.getHours();
        if (hours > 0) {
            if (hours > 1) {
                // align on multiples from origin
                long h = DEFAULT_CHRONOLOGY.hours().getDifferenceAsLong(t, origin);
                h -= h % hours;
                long tt = DEFAULT_CHRONOLOGY.hours().add(origin, h);
                // always round down to the previous period (for timestamps prior to origin)
                if (t < tt) {
                    t = DEFAULT_CHRONOLOGY.hours().add(tt, -hours);
                }
                else {
                    t = tt;
                }
                return t;
            }
            else {
                t = DEFAULT_CHRONOLOGY.minuteOfHour().roundFloor(t);
                return DEFAULT_CHRONOLOGY.minuteOfHour().set(t, 0);
            }
        }

        final int minutes = period.getMinutes();
        if (minutes > 0) {
            // align on multiples from origin
            if (minutes > 1) {
                long m = DEFAULT_CHRONOLOGY.minutes().getDifferenceAsLong(t, origin);
                m -= m % minutes;
                long tt = DEFAULT_CHRONOLOGY.minutes().add(origin, m);
                // always round down to the previous period (for timestamps prior to origin)
                if (t < tt) {
                    t = DEFAULT_CHRONOLOGY.minutes().add(tt, -minutes);
                }
                else {
                    t = tt;
                }
                return t;
            }
            else {
                t = DEFAULT_CHRONOLOGY.secondOfMinute().roundFloor(t);
                return DEFAULT_CHRONOLOGY.secondOfMinute().set(t, 0);
            }
        }

        final int seconds = period.getSeconds();
        if (seconds > 0) {
            // align on multiples from origin
            if (seconds > 1) {
                long s = DEFAULT_CHRONOLOGY.seconds().getDifferenceAsLong(t, origin);
                s -= s % seconds;
                long tt = DEFAULT_CHRONOLOGY.seconds().add(origin, s);
                // always round down to the previous period (for timestamps prior to origin)
                if (t < tt) {
                    t = DEFAULT_CHRONOLOGY.seconds().add(tt, -seconds);
                }
                else {
                    t = tt;
                }
                return t;
            }
            else {
                return DEFAULT_CHRONOLOGY.millisOfSecond().set(t, 0);
            }
        }

        final int millis = period.getMillis();
        if (millis > 0) {
            if (millis > 1) {
                long ms = DEFAULT_CHRONOLOGY.millis().getDifferenceAsLong(t, origin);
                ms -= ms % millis;
                long tt = DEFAULT_CHRONOLOGY.millis().add(origin, ms);
                // always round down to the previous period (for timestamps prior to origin)
                if (t < tt) {
                    t = DEFAULT_CHRONOLOGY.millis().add(tt, -millis);
                }
                else {
                    t = tt;
                }
                return t;
            }
            else {
                return t;
            }
        }
        log.info("======time_truncate: result=" + t);
        return t;
    }

    /**
     * <pre>
     *     m: minute,
     *     h: hour,
     *     d: day,
     *     w: week,
     *     M: month,
     *     y: year
     * </pre>
     *
     * @param factor
     * @param unit
     * @return
     */
    private static Period period(int factor, String unit)
    {
        switch (unit) {
            case "m":
                return Period.minutes(factor);
            case "h":
                return Period.hours(factor);
            case "d":
                return Period.days(factor);
            case "w":
                return Period.weeks(factor);
            case "M":
                return Period.months(factor);
            case "y":
                return Period.years(factor);
        }
        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "'" + unit + "' is not a valid Time field expected [nm,nh,nd,nw,nM,ny]");
    }
}
