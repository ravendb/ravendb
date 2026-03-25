using System;
using System.Runtime.CompilerServices;

namespace Raven.Server.Documents.Queries;

internal enum DateTimePrecision
{
    None = 0,
    Year = 1,
    Month = 2,
    Day = 3,
    Hour = 4,
    Minute = 5,
    Second = 6
}

internal readonly struct TimeFunctionOffset
{
    public readonly int Years;
    public readonly int Months;
    public readonly int Days;
    public readonly int Hours;
    public readonly int Minutes;
    public readonly int Seconds;
    public readonly bool IsNegative;
    public readonly DateTimePrecision SmallestUnit;

    private TimeFunctionOffset(int years, int months, int days, int hours, int minutes, int seconds, bool isNegative, DateTimePrecision smallestUnit)
    {
        Years = years;
        Months = months;
        Days = days;
        Hours = hours;
        Minutes = minutes;
        Seconds = seconds;
        IsNegative = isNegative;
        SmallestUnit = smallestUnit;
    }

    public static bool TryParse(ReadOnlySpan<char> input, out TimeFunctionOffset result)
    {
        result = default;
        input = input.Trim();
        if (input.IsEmpty) return false;

        int pos = 0;
        bool isNegative = false;

        // 1. Consume optional sign
        if (input[pos] == '-')
        {
            isNegative = true;
            pos++;
        }
        else if (input[pos] == '+')
        {
            pos++;
        }

        int years = 0, months = 0, days = 0, hours = 0, minutes = 0, seconds = 0;
        DateTimePrecision lastUnit = DateTimePrecision.None;

        while (pos < input.Length)
        {
            // Skip whitespace before numbers
            while (pos < input.Length && char.IsWhiteSpace(input[pos])) pos++;
            if (pos >= input.Length) break;

            // 2. Parse numeric value (stepping)
            if (TryParseStep(input.Slice(pos), out long value, out int numConsumed) == false)
                return false;

            pos += numConsumed;

            // Skip whitespace between number and unit
            while (pos < input.Length && char.IsWhiteSpace(input[pos])) pos++;
            if (pos >= input.Length) return false; // Digits without a unit suffix

            // 3. Parse unit suffix
            if (TryMatchUnit(input.Slice(pos), out DateTimePrecision currentUnit, out int unitConsumed) == false)
                return false;

            // 4. Enforce strict ordering (e.g., cannot have 'days' before 'years')
            if (currentUnit <= lastUnit) return false;

            switch (currentUnit)
            {
                case DateTimePrecision.Year: years = (int)value; break;
                case DateTimePrecision.Month: months = (int)value; break;
                case DateTimePrecision.Day: days = (int)value; break;
                case DateTimePrecision.Hour: hours = (int)value; break;
                case DateTimePrecision.Minute: minutes = (int)value; break;
                case DateTimePrecision.Second: seconds = (int)value; break;
            }

            lastUnit = currentUnit;
            pos += unitConsumed;
        }

        if (lastUnit == DateTimePrecision.None) return false;

        result = new TimeFunctionOffset(years, months, days, hours, minutes, seconds, isNegative, lastUnit);
        return true;
    }

    private static bool TryParseStep(ReadOnlySpan<char> source, out long value, out int charsConsumed)
    {
        charsConsumed = 0;
        while (charsConsumed < source.Length && char.IsDigit(source[charsConsumed]))
            charsConsumed++;

        if (charsConsumed == 0)
        {
            value = 0;
            return false;
        }

        return long.TryParse(source.Slice(0, charsConsumed), out value);
    }

    private static bool TryMatchUnit(ReadOnlySpan<char> input, out DateTimePrecision unit, out int consumed)
    {
        unit = DateTimePrecision.None;
        consumed = 0;

        // Longest strings first to avoid greedy matching errors (e.g., 'm' matching 'month')
        if (StartsWith(input, out consumed, ["years", "year", "y"])) unit = DateTimePrecision.Year;
        else if (StartsWith(input, out consumed, ["months", "month", "mo"])) unit = DateTimePrecision.Month;
        else if (StartsWith(input, out consumed, ["days", "day", "d"])) unit = DateTimePrecision.Day;
        else if (StartsWith(input, out consumed, ["hours", "hour", "h"])) unit = DateTimePrecision.Hour;
        else if (StartsWith(input, out consumed, ["minutes", "minute", "min", "m"])) unit = DateTimePrecision.Minute;
        else if (StartsWith(input, out consumed, ["seconds", "second", "sec", "s"])) unit = DateTimePrecision.Second;

        return unit != DateTimePrecision.None;
    }

    private static bool StartsWith(ReadOnlySpan<char> input, out int length, ReadOnlySpan<string> options)
    {
        foreach (var opt in options)
        {
            if (input.StartsWith(opt, StringComparison.OrdinalIgnoreCase))
            {
                length = opt.Length;
                return true;
            }
        }
        length = 0;
        return false;
    }

    public DateTime Apply(DateTime baseTime)
    {
        int sign = IsNegative ? -1 : 1;
        var result = baseTime;

        if (Years != 0) result = result.AddYears(Years * sign);
        if (Months != 0) result = result.AddMonths(Months * sign);
        if (Days != 0) result = result.AddDays(Days * sign);
        if (Hours != 0) result = result.AddHours(Hours * sign);
        if (Minutes != 0) result = result.AddMinutes(Minutes * sign);
        if (Seconds != 0) result = result.AddSeconds(Seconds * sign);

        return FloorTo(result, SmallestUnit);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static DateTime FloorTo(DateTime dt, DateTimePrecision precision)
    {
        return precision switch
        {
            DateTimePrecision.Year => new DateTime(dt.Year, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            DateTimePrecision.Month => new DateTime(dt.Year, dt.Month, 1, 0, 0, 0, DateTimeKind.Utc),
            DateTimePrecision.Day => new DateTime(dt.Year, dt.Month, dt.Day, 0, 0, 0, DateTimeKind.Utc),
            DateTimePrecision.Hour => new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, 0, 0, DateTimeKind.Utc),
            DateTimePrecision.Minute => new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, 0, DateTimeKind.Utc),
            DateTimePrecision.Second => new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, DateTimeKind.Utc),
            _ => dt
        };
    }
}
