using System;
using System.Runtime.CompilerServices;

namespace Raven.Server.Documents.Queries;

internal enum DateTimePrecision : byte
{
    None = 0,
    Year = 1,
    Month = 2,
    Day = 3,
    Hour = 4,
    Minute = 5,
    Second = 6
}

/// <summary>
/// Parses and applies time offsets for now()/today() RQL functions.
/// Format: [+|-]NyNmoNdNhNmNs (e.g., "+1y7mo5d4h5m7s", "-1d", "1h30m")
/// The result is floor-rounded to the smallest precision unit specified.
/// </summary>
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

        if (input.IsEmpty)
            return false;

        int pos = 0;
        bool isNegative = false;

        // consume optional sign
        if (input[pos] == '+')
        {
            pos++;
        }
        else if (input[pos] == '-')
        {
            isNegative = true;
            pos++;
        }

        if (pos >= input.Length)
            return false;

        int years = 0, months = 0, days = 0, hours = 0, minutes = 0, seconds = 0;
        var smallestUnit = DateTimePrecision.None;
        bool hasAnyUnit = false;

        while (pos < input.Length)
        {
            // parse numeric value
            int numStart = pos;
            int value = 0;

            while (pos < input.Length && IsDigit(input[pos]))
            {
                value = value * 10 + (input[pos] - '0');
                pos++;
            }

            if (pos == numStart || pos >= input.Length)
                return false; // no digits found or no unit suffix

            // parse unit suffix
            char unit = input[pos];
            switch (unit)
            {
                case 'y':
                case 'Y':
                    years = value;
                    pos++;
                    if (smallestUnit < DateTimePrecision.Year)
                        smallestUnit = DateTimePrecision.Year;
                    break;

                case 'm':
                case 'M':
                    // lookahead: 'mo' = months, bare 'm' = minutes
                    if (pos + 1 < input.Length && (input[pos + 1] == 'o' || input[pos + 1] == 'O'))
                    {
                        months = value;
                        pos += 2;
                        if (smallestUnit < DateTimePrecision.Month)
                            smallestUnit = DateTimePrecision.Month;
                    }
                    else
                    {
                        minutes = value;
                        pos++;
                        if (smallestUnit < DateTimePrecision.Minute)
                            smallestUnit = DateTimePrecision.Minute;
                    }
                    break;

                case 'd':
                case 'D':
                    days = value;
                    pos++;
                    if (smallestUnit < DateTimePrecision.Day)
                        smallestUnit = DateTimePrecision.Day;
                    break;

                case 'h':
                case 'H':
                    hours = value;
                    pos++;
                    if (smallestUnit < DateTimePrecision.Hour)
                        smallestUnit = DateTimePrecision.Hour;
                    break;

                case 's':
                case 'S':
                    seconds = value;
                    pos++;
                    if (smallestUnit < DateTimePrecision.Second)
                        smallestUnit = DateTimePrecision.Second;
                    break;

                default:
                    return false; // unknown unit
            }

            hasAnyUnit = true;
        }

        if (hasAnyUnit == false)
            return false;

        result = new TimeFunctionOffset(years, months, days, hours, minutes, seconds, isNegative, smallestUnit);
        return true;
    }

    /// <summary>
    /// Applies the offset to the base time and floor-rounds to the smallest precision unit.
    /// </summary>
    public DateTime Apply(DateTime baseTime)
    {
        int sign = IsNegative ? -1 : 1;

        var result = baseTime;

        if (Years != 0)
            result = result.AddYears(Years * sign);
        if (Months != 0)
            result = result.AddMonths(Months * sign);
        if (Days != 0)
            result = result.AddDays(Days * sign);
        if (Hours != 0)
            result = result.AddHours(Hours * sign);
        if (Minutes != 0)
            result = result.AddMinutes(Minutes * sign);
        if (Seconds != 0)
            result = result.AddSeconds(Seconds * sign);

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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsDigit(char c) => (uint)(c - '0') <= 9;
}
