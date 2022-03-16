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

import com.google.common.collect.ObjectArrays;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeZoneKey;
import org.joda.time.DateTime;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Locale;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.TimeZoneKey.MAX_TIME_ZONE_KEY;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeys;
import static io.trino.udf.function.TimeFunctions.scaleEpochMicrosToMillis;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author shenlongguang https://github.com/ifengkou
 * @date: 2021/10/30
 */
public final class DateTimeExtFunctions
{
    private static final Logger log = Logger.get(DateTimeExtFunctions.class);

    private static final DateTimeZone[] DATE_TIME_ZONES;
    private static final ISOChronology[] CHRONOLOGIES;
    private static final int[] FIXED_ZONE_OFFSET;

    private static final int VARIABLE_ZONE = Integer.MAX_VALUE;

    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstanceUTC();

    private static final DateTimeField DAY_OF_MONTH = UTC_CHRONOLOGY.dayOfMonth();

    public static final String[] STANDARD_DATETIME_PATTERN = new String[]
            {"yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss", "yyyyMMddHHmmss",
                    "yyyy-MM-dd HH:mm:ss SSS", "HH:mm:ss", "yyyy-MM-dd HH:mm"};
    public static final String[] STANDARD_DATE_PATTERN = new String[] {"yyyy-MM-dd", "yyyy/MM/dd", "yyyyMMdd"};
    public static final String[] STANDARD_ALL_PATTERN;

    private static final int TIME_ZONE_MASK = 0xFFF;
    private static final int MILLIS_SHIFT = 12;
    public static final int MICROSECONDS_PER_MILLISECOND = 1000;

    private DateTimeExtFunctions()
    {
    }

    static {
        DATE_TIME_ZONES = new DateTimeZone[MAX_TIME_ZONE_KEY + 1];
        CHRONOLOGIES = new ISOChronology[MAX_TIME_ZONE_KEY + 1];
        FIXED_ZONE_OFFSET = new int[MAX_TIME_ZONE_KEY + 1];
        for (TimeZoneKey timeZoneKey : getTimeZoneKeys()) {
            short zoneKey = timeZoneKey.getKey();
            DateTimeZone dateTimeZone = DateTimeZone.forID(timeZoneKey.getId());
            DATE_TIME_ZONES[zoneKey] = dateTimeZone;
            CHRONOLOGIES[zoneKey] = ISOChronology.getInstance(dateTimeZone);
            if (dateTimeZone.isFixed() && dateTimeZone.getOffset(0) % 60_000 == 0) {
                FIXED_ZONE_OFFSET[zoneKey] = dateTimeZone.getOffset(0) / 60_000;
            }
            else {
                FIXED_ZONE_OFFSET[zoneKey] = VARIABLE_ZONE;
            }
        }
        STANDARD_ALL_PATTERN = ObjectArrays.concat(STANDARD_DATE_PATTERN, STANDARD_DATETIME_PATTERN, String.class);
    }

    @Description("Current date")
    @ScalarFunction("today")
    @SqlType(StandardTypes.DATE)
    public static long currentDate(ConnectorSession session)
    {
        ISOChronology chronology = getChronology(session.getTimeZoneKey());
        LocalDate currentDate = new DateTime(session.getStart().toEpochMilli(), chronology).toLocalDate();
        return Days.daysBetween(new LocalDate(1970, 1, 1), currentDate).getDays();
    }

    @Description("Parse the specified date by give (year,month,day)")
    @ScalarFunction("date")
    @SqlType(StandardTypes.DATE)
    public static long parseDate(@SqlType(StandardTypes.INTEGER) long year, @SqlType(StandardTypes.INTEGER) long month, @SqlType(StandardTypes.INTEGER) long day)
    {
        DateTime dateTime = new DateTime(toIntExact(year), toIntExact(month), toIntExact(day), 0, 0, 0);
        LocalDate localDate = dateTime.toLocalDate();
        return Days.daysBetween(new LocalDate(1970, 1, 1), localDate).getDays();
    }

    @Description("Parse the specified date by give (year,month,day)")
    @ScalarFunction("time")
    @SqlType("timestamp(3)")
    public static long parseTime(@SqlType(StandardTypes.INTEGER) long hour,
            @SqlType(StandardTypes.INTEGER) long minute,
            @SqlType(StandardTypes.INTEGER) long second)
    {
        DateTime dateTime = new DateTime();
        dateTime = dateTime.withHourOfDay(toIntExact(hour))
                .withMinuteOfHour(toIntExact(minute))
                .withSecondOfMinute(toIntExact(second))
                .withMillisOfSecond(0);
        //带时区的 return packDateTimeWithZone(dateTime);
        return scaleEpochMillisToMicros(dateTime.getMillis());
    }

    @Description("Year of the given date(str)")
    @ScalarFunction(value = "year")
    @SqlType(StandardTypes.INTEGER)
    public static long yearFromDate(@SqlType(StandardTypes.VARCHAR) Slice datetime)
    {
        DateTime dateTime = parseDateTime(datetime.toStringUtf8(), STANDARD_ALL_PATTERN);
        return dateTime.getYear();
    }

    @Description("Year of the current_date")
    @ScalarFunction(value = "year")
    @SqlType(StandardTypes.INTEGER)
    public static long yearFromNow()
    {
        DateTime dateTime = new DateTime();
        return dateTime.getYear();
    }

    @Description("Quarter of the year of the given date(str)")
    @ScalarFunction(value = "quarter", alias = "quarter_of_year")
    @LiteralParameters("x")
    @SqlType(StandardTypes.INTEGER)
    public static long quarterOfYear(@SqlType("varchar(x)") Slice datetime)
    {
        DateTime dateTime = parseDateTime(datetime.toStringUtf8(), STANDARD_ALL_PATTERN);
        return (dateTime.getMonthOfYear() / 4 + 1);
    }

    @Description("Quarter of the year of the current_date")
    @ScalarFunction(value = "quarter", alias = "quarter_of_year")
    @SqlType(StandardTypes.INTEGER)
    public static long quarterOfYear()
    {
        DateTime dateTime = new DateTime();
        return (dateTime.getMonthOfYear() / 4 + 1);
    }

    @Description("Month of the year of the given date(str)")
    @ScalarFunction(value = "month", alias = "month_of_year")
    @LiteralParameters("x")
    @SqlType(StandardTypes.INTEGER)
    public static long monthOfYear(@SqlType("varchar(x)") Slice datetime)
    {
        DateTime dateTime = parseDateTime(datetime.toStringUtf8(), STANDARD_ALL_PATTERN);
        return dateTime.getMonthOfYear();
    }

    @Description("Month of the year of the current_date")
    @ScalarFunction(value = "month", alias = "month_of_year")
    @SqlType(StandardTypes.INTEGER)
    public static long monthOfYear()
    {
        DateTime dateTime = new DateTime();
        return dateTime.getMonthOfYear();
    }

    @Description("Week of the year of the given date(str)")
    @ScalarFunction(value = "week", alias = "week_of_year")
    @LiteralParameters("x")
    @SqlType(StandardTypes.INTEGER)
    public static long weekOfYear(@SqlType("varchar(x)") Slice datetime)
    {
        DateTime dateTime = parseDateTime(datetime.toStringUtf8(), STANDARD_ALL_PATTERN);
        return dateTime.getWeekOfWeekyear();
    }

    @Description("Week of the year of the current_date")
    @ScalarFunction(value = "week", alias = "week_of_year")
    @SqlType(StandardTypes.INTEGER)
    public static long weekOfYear()
    {
        DateTime dateTime = new DateTime();
        return dateTime.getWeekOfWeekyear();
    }

    @Description("weekday of the given date(str)")
    @ScalarFunction(value = "weekday")
    @LiteralParameters("x")
    @SqlType(StandardTypes.INTEGER)
    public static long weekday(@SqlType("varchar(x)") Slice datetime)
    {
        DateTime dateTime = parseDateTime(datetime.toStringUtf8(), STANDARD_ALL_PATTERN);
        int week = dateTime.getDayOfWeek();
        week = week == 7 ? 0 : week;
        return week;
    }

    @Description("Weekday of the current_date")
    @ScalarFunction(value = "weekday")
    @SqlType(StandardTypes.INTEGER)
    public static long weekday()
    {
        DateTime dateTime = new DateTime();
        int week = dateTime.getDayOfWeek();
        week = week == 7 ? 0 : week;
        return week;
    }

    @Description("Day of the week of the given date(date) ")
    @ScalarFunction(value = "weekday", alias = "day_of_week")
    @SqlType(StandardTypes.INTEGER)
    public static long weekday(@SqlType(StandardTypes.DATE) long date)
    {
        long mills = DAYS.toMillis(date);
        DateTime dateTime = new DateTime(mills);
        int week = dateTime.getDayOfWeek();
        week = week == 7 ? 0 : week;
        return week;
    }

    @Description("Day of the week of the given timestamp(3)")
    @ScalarFunction(value = "weekday")
    @LiteralParameters("p")
    @SqlType(StandardTypes.INTEGER)
    public static long weekdayFromTimestamp(@SqlType("timestamp(p)") long timestamp)
    {
        long mills = scaleEpochMicrosToMillis(timestamp);
        DateTime dateTime = new DateTime(mills);
        int week = dateTime.getDayOfWeek();
        week = week == 7 ? 0 : week;
        return week;
    }

    @Description("Day of the week of the given timestamp(3)")
    @ScalarFunction(value = "weekday")
    @LiteralParameters("p")
    @SqlType(StandardTypes.INTEGER)
    public static long weekdayFromTimestampTz(@SqlType("timestamp(p) with time zone") long packedEpochMillis)
    {
        ISOChronology chronology = getChronology(unpackZoneKey(packedEpochMillis));
        int week = chronology.dayOfWeek().get(unpackMillisUtc(packedEpochMillis));
        week = week == 7 ? 0 : week;
        return week;
    }

    public static TimeZoneKey unpackZoneKey(long dateTimeWithTimeZone)
    {
        return getTimeZoneKey((short) (dateTimeWithTimeZone & TIME_ZONE_MASK));
    }

    @Description("Day of the month of current time")
    @ScalarFunction(value = "day", alias = "day_of_month")
    @SqlType(StandardTypes.INTEGER)
    public static long dayFromNow()
    {
        DateTime now = new DateTime();
        return now.getDayOfMonth();
    }

    @Description("Day of the month of the given date(str)")
    @ScalarFunction(value = "day", alias = "day_of_month")
    @LiteralParameters("x")
    @SqlType(StandardTypes.INTEGER)
    public static long dayFromDate(@SqlType("varchar(x)") Slice datetime)
    {
        DateTime dateTime = parseDateTime(datetime.toStringUtf8(), STANDARD_ALL_PATTERN);
        //return DAY_OF_MONTH.get(dateTime.getMillis());
        return dateTime.getDayOfMonth();
    }

    @Description("Day of the month of the unixMills(13)")
    @ScalarFunction(value = "day", alias = "day_of_month")
    @SqlType(StandardTypes.INTEGER)
    public static long dayFromUnixTime(ConnectorSession session, @SqlType(StandardTypes.DOUBLE) double unixMills)
    {
        // TODO (https://github.com/trinodb/trino/issues/5781)
        long mills = packDateTimeWithZone(Math.round(unixMills), session.getTimeZoneKey());
        return DAY_OF_MONTH.get(mills);
    }

    @Description("Hour of the day of the current time ")
    @ScalarFunction("hour")
    @SqlType(StandardTypes.INTEGER)
    public static long hourFromNow()
    {
        DateTime now = new DateTime();
        int hour = now.getHourOfDay();
        return hour;
    }

    @Description("Hour of the day of the given date_str")
    @ScalarFunction("hour")
    @LiteralParameters("x")
    @SqlType(StandardTypes.INTEGER)
    public static long hourFromDate(@SqlType("varchar(x)") Slice datetime)
    {
        DateTime dateTime = parseDateTime(datetime.toStringUtf8(), STANDARD_ALL_PATTERN);
        int hour = dateTime.getHourOfDay();
        return hour;
    }

    @Description("Hour of the day of the time(timestamp) ")
    @ScalarFunction("hour")
    @SqlType(StandardTypes.INTEGER)
    public static long hourFromDate(@SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        long mills = scaleEpochMicrosToMillis(timestamp);
        DateTime time = new DateTime(mills);
        return time.getHourOfDay();
    }

    @Description("minute of the day of the current time ")
    @ScalarFunction("minute")
    @SqlType(StandardTypes.INTEGER)
    public static long minuteFromNow()
    {
        DateTime now = new DateTime();
        return now.getMinuteOfHour();
    }

    @Description("Minute of the day of the given date_str")
    @ScalarFunction("minute")
    @LiteralParameters("x")
    @SqlType(StandardTypes.INTEGER)
    public static long minuteFromDate(@SqlType("varchar(x)") Slice datetime)
    {
        DateTime dateTime = parseDateTime(datetime.toStringUtf8(), STANDARD_ALL_PATTERN);
        return dateTime.getMinuteOfHour();
    }

    @Description("Minute of the day of the time(timestamp) ")
    @ScalarFunction("minute")
    @SqlType(StandardTypes.INTEGER)
    public static long minuteFromDate(@SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        long mills = scaleEpochMicrosToMillis(timestamp);
        DateTime time = new DateTime(mills);
        return time.getMinuteOfHour();
    }

    @Description("Second of the day of the current time ")
    @ScalarFunction("second")
    @SqlType(StandardTypes.INTEGER)
    public static long secondFromNow()
    {
        DateTime now = new DateTime();
        int hour = now.getSecondOfMinute();
        return hour;
    }

    @Description("Second of the day of the given date_str")
    @ScalarFunction("second")
    @LiteralParameters("x")
    @SqlType(StandardTypes.INTEGER)
    public static long secondFromDate(@SqlType("varchar(x)") Slice datetime)
    {
        DateTime dateTime = parseDateTime(datetime.toStringUtf8(), STANDARD_ALL_PATTERN);
        return dateTime.getSecondOfMinute();
    }

    @Description("Second of the day of the time(timestamp) ")
    @ScalarFunction("second")
    @SqlType(StandardTypes.INTEGER)
    public static long secondFromDate(@SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        long mills = scaleEpochMicrosToMillis(timestamp);
        DateTime time = new DateTime(mills);
        return time.getSecondOfMinute();
    }

    @Description("Delta days")
    @ScalarFunction("date_delta")
    @LiteralParameters("x")
    @SqlType(StandardTypes.DATE)
    public static long deltaDate(@SqlType("varchar(x)") Slice datetime, @SqlType(StandardTypes.BIGINT) long days)
    {
        DateTime time = parseDateTime(datetime.toStringUtf8(), STANDARD_DATE_PATTERN);
        /* method 1
        time.withFieldAdded(DurationFieldType.days(), toIntExact(days));
        return packDateTimeWithZone(time);*/
        // method 2
        Slice unit = Slices.utf8Slice("day");
        long millis = getDateField(UTC_CHRONOLOGY, unit).add(time.getMillis(), toIntExact(days));
        //return MILLISECONDS.toDays(millis);
        DateTime dateTime = new DateTime(millis);
        LocalDate localDate = dateTime.toLocalDate();
        return Days.daysBetween(new LocalDate(1970, 1, 1), localDate).getDays();
    }

    @Description("Delta days")
    @ScalarFunction("date_delta")
    @LiteralParameters("x")
    @SqlType(StandardTypes.DATE)
    public static long deltaDate(@SqlType(StandardTypes.DATE) long date, @SqlType(StandardTypes.BIGINT) long days)
    {
        Slice unit = Slices.utf8Slice("day");
        long millis = getDateField(UTC_CHRONOLOGY, unit).add(DAYS.toMillis(date), toIntExact(days));
        DateTime dateTime = new DateTime(millis);
        LocalDate localDate = dateTime.toLocalDate();
        return Days.daysBetween(new LocalDate(1970, 1, 1), localDate).getDays();
    }

    @Description("Difference of the given dates(str) in the given unit(day/month/year)")
    @ScalarFunction("date_diff")
    @LiteralParameters({"x", "y", "z"})
    @SqlType(StandardTypes.BIGINT)
    public static long diffDate(@SqlType("varchar(x)") Slice unit, @SqlType("varchar(y)") Slice date1, @SqlType("varchar(z)") Slice date2)
    {
        DateTime time1 = parseDateTime(date1.toStringUtf8(), STANDARD_DATE_PATTERN);
        DateTime time2 = parseDateTime(date2.toStringUtf8(), STANDARD_DATE_PATTERN);
        return getDateField(UTC_CHRONOLOGY, unit).getDifferenceAsLong(time2.getMillis(), time1.getMillis());
    }

    @Description("Parses the specified date/time by the given format")
    @ScalarFunction("to_date")
    @LiteralParameters("x")
    @SqlType(StandardTypes.DATE)
    public static long parseDatetime(ConnectorSession session, @SqlType("varchar(x)") Slice datetime)
    {
        DateTime dateTime = parseDateTime(datetime.toStringUtf8(),
                STANDARD_ALL_PATTERN,
                getChronology(session.getTimeZoneKey()),
                session.getLocale());
        log.debug("mills=" + dateTime.getMillis());
        log.debug("mills.todays=" + MILLISECONDS.toDays(dateTime.getMillis()));
        //return MILLISECONDS.toDays(time.getMillis()); //存在时区问题
        LocalDate localDate = dateTime.toLocalDate();
        return Days.daysBetween(new LocalDate(1970, 1, 1), localDate).getDays();
    }

    @Description("Parses the specified date/time by the given unixMills(13)")
    @ScalarFunction("to_date")
    @SqlType(StandardTypes.DATE)
    public static long parseDateWithUnixMills(@SqlType(StandardTypes.DOUBLE) double unixMills)
    {
        DateTime time = new DateTime(Math.round(unixMills));
        time.withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0);
        LocalDate localDate = time.toLocalDate();
        return Days.daysBetween(new LocalDate(1970, 1, 1), localDate).getDays();
    }

    @Description("Parses the specified date/time by timestamp(p)")
    @LiteralParameters("p")
    @ScalarFunction("to_date")
    @SqlType(StandardTypes.DATE)
    public static long parseDateWithTimestamp(@SqlType("timestamp(p)") long timestamp)
    {
        long mills = scaleEpochMicrosToMillis(timestamp);
        DateTime time = new DateTime(mills);
        LocalDate localDate = time.toLocalDate();
        return Days.daysBetween(new LocalDate(1970, 1, 1), localDate).getDays();
    }

    @Description("Parses the specified date/time by timestamp(p) with time zone")
    @LiteralParameters("p")
    @ScalarFunction("to_date")
    @SqlType(StandardTypes.DATE)
    public static long parseDateWithTimestampTz(@SqlType("timestamp(p) with time zone") long packedEpochMillis)
    {
        ISOChronology chronology = getChronology(unpackZoneKey(packedEpochMillis));
        LocalDate currentDate = new DateTime(unpackMillisUtc(packedEpochMillis), chronology).toLocalDate();
        return Days.daysBetween(new LocalDate(1970, 1, 1), currentDate).getDays();
    }

    @Description("Parse a date(return original date)")
    @ScalarFunction("to_date")
    @SqlType(StandardTypes.DATE)
    public static long parseDate(@SqlType(StandardTypes.DATE) long date)
    {
        return date;
    }

    public static ISOChronology getChronology(TimeZoneKey zoneKey)
    {
        return CHRONOLOGIES[zoneKey.getKey()];
    }

    public static long packDateTimeWithZone(DateTime dateTime)
    {
        return DateTimeEncoding.packDateTimeWithZone(dateTime.getMillis(), dateTime.getZone().getID());
    }

    public static long packDateTimeWithZone(long millisUtc, TimeZoneKey timeZoneKey)
    {
        requireNonNull(timeZoneKey, "timeZoneKey is null");
        return pack(millisUtc, timeZoneKey.getKey());
    }

    private static long pack(long millisUtc, short timeZoneKey)
    {
        if (millisUtc << MILLIS_SHIFT >> MILLIS_SHIFT != millisUtc) {
            throw new IllegalArgumentException("Millis overflow: " + millisUtc);
        }

        return (millisUtc << MILLIS_SHIFT) | (timeZoneKey & TIME_ZONE_MASK);
    }

    public static long scaleEpochMillisToMicros(long epochMillis)
    {
        return multiplyExact(epochMillis, MICROSECONDS_PER_MILLISECOND);
    }

    public static DateTime parseDateTime(String dateTimeString, String[] formatter)
    {
        if (formatter == null || formatter.length == 0) {
            formatter = STANDARD_ALL_PATTERN;
        }
        int length = dateTimeString.length();
        for (String f : formatter) {
            if (length == f.length()) {
                DateTimeFormatter ft = DateTimeFormat.forPattern(f);
                try {
                    return parseDateTimeHelper(ft, dateTimeString);
                }
                catch (TrinoException e) {
                    continue;
                }
            }
        }
        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Cannot parse [" + dateTimeString + "] to a DateTime");
    }

    public static DateTime parseDateTime(String dateTimeString, String[] formatter, ISOChronology chronology, Locale locale)
    {
        if (formatter == null || formatter.length == 0) {
            formatter = STANDARD_ALL_PATTERN;
        }
        int length = dateTimeString.length();
        for (String f : formatter) {
            if (length == f.length()) {
                DateTimeFormatter ft = DateTimeFormat.forPattern(f)
                        .withChronology(chronology)
                        .withOffsetParsed()
                        .withLocale(locale);
                try {
                    return parseDateTimeHelper(ft, dateTimeString);
                }
                catch (TrinoException e) {
                    continue;
                }
            }
        }
        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Cannot parse [" + dateTimeString + "] to a DateTime");
    }

    private static DateTime parseDateTimeHelper(DateTimeFormatter formatter, String datetimeString)
    {
        try {
            return formatter.parseDateTime(datetimeString);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    private static DateTimeField getDateField(ISOChronology chronology, Slice unit)
    {
        String unitString = unit.toStringUtf8().toLowerCase(ENGLISH);
        switch (unitString) {
            case "day":
                return chronology.dayOfMonth();
            case "week":
                return chronology.weekOfWeekyear();
            case "month":
                return chronology.monthOfYear();
            case "year":
                return chronology.year();
        }
        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "'" + unitString + "' is not a valid DATE field");
    }
}
