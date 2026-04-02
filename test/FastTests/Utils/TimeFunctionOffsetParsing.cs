using System;
using Raven.Server.Documents.Queries;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Utils
{
    public class TimeFunctionOffsetParsing(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenFact(RavenTestCategory.Querying)]
        public void CanParseSingleUnit_Years()
        {
            Assert.True(TimeFunctionOffset.TryParse("7y", out var result));
            Assert.Equal(7, result.Years);
            Assert.Equal(DateTimePrecision.Year, result.SmallestUnit);
            Assert.False(result.IsNegative);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void CanParseSingleUnit_Months()
        {
            Assert.True(TimeFunctionOffset.TryParse("3mo", out var result));
            Assert.Equal(3, result.Months);
            Assert.Equal(DateTimePrecision.Month, result.SmallestUnit);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void CanParseSingleUnit_Days()
        {
            Assert.True(TimeFunctionOffset.TryParse("5d", out var result));
            Assert.Equal(5, result.Days);
            Assert.Equal(DateTimePrecision.Day, result.SmallestUnit);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void CanParseSingleUnit_Hours()
        {
            Assert.True(TimeFunctionOffset.TryParse("4h", out var result));
            Assert.Equal(4, result.Hours);
            Assert.Equal(DateTimePrecision.Hour, result.SmallestUnit);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void CanParseSingleUnit_Minutes()
        {
            Assert.True(TimeFunctionOffset.TryParse("30m", out var result));
            Assert.Equal(30, result.Minutes);
            Assert.Equal(DateTimePrecision.Minute, result.SmallestUnit);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void CanParseSingleUnit_Seconds()
        {
            Assert.True(TimeFunctionOffset.TryParse("15s", out var result));
            Assert.Equal(15, result.Seconds);
            Assert.Equal(DateTimePrecision.Second, result.SmallestUnit);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void CanParseAllUnits()
        {
            Assert.True(TimeFunctionOffset.TryParse("1y7mo5d4h5m7s", out var result));
            Assert.Equal(1, result.Years);
            Assert.Equal(7, result.Months);
            Assert.Equal(5, result.Days);
            Assert.Equal(4, result.Hours);
            Assert.Equal(5, result.Minutes);
            Assert.Equal(7, result.Seconds);
            Assert.Equal(DateTimePrecision.Second, result.SmallestUnit);
            Assert.False(result.IsNegative);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void CanParsePositiveSign()
        {
            Assert.True(TimeFunctionOffset.TryParse("+1d5h", out var result));
            Assert.Equal(1, result.Days);
            Assert.Equal(5, result.Hours);
            Assert.False(result.IsNegative);
            Assert.Equal(DateTimePrecision.Hour, result.SmallestUnit);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void CanParseNegativeSign()
        {
            Assert.True(TimeFunctionOffset.TryParse("-1d5h", out var result));
            Assert.Equal(1, result.Days);
            Assert.Equal(5, result.Hours);
            Assert.True(result.IsNegative);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void SmallestUnit_TracksFinestPrecision()
        {
            // 7y has year precision, 0s has second precision -> smallest is second
            Assert.True(TimeFunctionOffset.TryParse("7y0s", out var result));
            Assert.Equal(7, result.Years);
            Assert.Equal(0, result.Seconds);
            Assert.Equal(DateTimePrecision.Second, result.SmallestUnit);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void StrictOrdering_RejectsOutOfOrder()
        {
            // hours before days violates strict descending order
            Assert.False(TimeFunctionOffset.TryParse("5h1d", out _));
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void StrictOrdering_RejectsDuplicateUnits()
        {
            Assert.False(TimeFunctionOffset.TryParse("1d2d", out _));
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void CanDistinguish_Minutes_From_Months()
        {
            Assert.True(TimeFunctionOffset.TryParse("5mo10m", out var result));
            Assert.Equal(5, result.Months);
            Assert.Equal(10, result.Minutes);
            Assert.Equal(DateTimePrecision.Minute, result.SmallestUnit);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void CanParseReadableUnitNames()
        {
            Assert.True(TimeFunctionOffset.TryParse("1year6months5days", out var result));
            Assert.Equal(1, result.Years);
            Assert.Equal(6, result.Months);
            Assert.Equal(5, result.Days);
            Assert.Equal(DateTimePrecision.Day, result.SmallestUnit);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void CanParseSingularUnitNames()
        {
            Assert.True(TimeFunctionOffset.TryParse("1year1month1day1hour1minute1second", out var result));
            Assert.Equal(1, result.Years);
            Assert.Equal(1, result.Months);
            Assert.Equal(1, result.Days);
            Assert.Equal(1, result.Hours);
            Assert.Equal(1, result.Minutes);
            Assert.Equal(1, result.Seconds);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void CanParseShortAliases_MinSec()
        {
            Assert.True(TimeFunctionOffset.TryParse("5min30sec", out var result));
            Assert.Equal(5, result.Minutes);
            Assert.Equal(30, result.Seconds);
            Assert.Equal(DateTimePrecision.Second, result.SmallestUnit);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void CanParseWithWhitespace()
        {
            Assert.True(TimeFunctionOffset.TryParse("1y 6mo 5d", out var result));
            Assert.Equal(1, result.Years);
            Assert.Equal(6, result.Months);
            Assert.Equal(5, result.Days);
            Assert.Equal(DateTimePrecision.Day, result.SmallestUnit);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void CanParseWithLeadingTrailingWhitespace()
        {
            Assert.True(TimeFunctionOffset.TryParse("  +1d  ", out var result));
            Assert.Equal(1, result.Days);
            Assert.False(result.IsNegative);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void CaseInsensitiveUnits()
        {
            Assert.True(TimeFunctionOffset.TryParse("1Y2MO3D4H5M6S", out var result));
            Assert.Equal(1, result.Years);
            Assert.Equal(2, result.Months);
            Assert.Equal(3, result.Days);
            Assert.Equal(4, result.Hours);
            Assert.Equal(5, result.Minutes);
            Assert.Equal(6, result.Seconds);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void CaseInsensitiveReadableNames()
        {
            Assert.True(TimeFunctionOffset.TryParse("1Year 2Months", out var result));
            Assert.Equal(1, result.Years);
            Assert.Equal(2, result.Months);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void FailsOnEmptyString()
        {
            Assert.False(TimeFunctionOffset.TryParse("", out _));
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void FailsOnSignOnly()
        {
            Assert.False(TimeFunctionOffset.TryParse("+", out _));
            Assert.False(TimeFunctionOffset.TryParse("-", out _));
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void FailsOnUnknownUnit()
        {
            Assert.False(TimeFunctionOffset.TryParse("5x", out _));
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void FailsOnMissingUnit()
        {
            Assert.False(TimeFunctionOffset.TryParse("5", out _));
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void FailsOnMissingNumber()
        {
            Assert.False(TimeFunctionOffset.TryParse("d", out _));
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void Apply_AddsYearsAndFloorsToYear()
        {
            Assert.True(TimeFunctionOffset.TryParse("+7y", out var offset));
            var baseTime = new DateTime(2026, 6, 15, 14, 30, 45, DateTimeKind.Utc);
            var result = offset.Apply(baseTime);
            Assert.Equal(new DateTime(2033, 1, 1, 0, 0, 0, DateTimeKind.Utc), result);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void Apply_AddsMonthsAndFloorsToMonth()
        {
            Assert.True(TimeFunctionOffset.TryParse("+1mo", out var offset));
            var baseTime = new DateTime(2026, 6, 15, 14, 30, 45, DateTimeKind.Utc);
            var result = offset.Apply(baseTime);
            Assert.Equal(new DateTime(2026, 7, 1, 0, 0, 0, DateTimeKind.Utc), result);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void Apply_AddsDaysAndHoursAndFloorsToHour()
        {
            Assert.True(TimeFunctionOffset.TryParse("+1d5h", out var offset));
            var baseTime = new DateTime(2026, 3, 22, 14, 30, 45, DateTimeKind.Utc);
            var result = offset.Apply(baseTime);
            Assert.Equal(new DateTime(2026, 3, 23, 19, 0, 0, DateTimeKind.Utc), result);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void Apply_YearsWithSecondPrecision_FloorsToSecond()
        {
            Assert.True(TimeFunctionOffset.TryParse("7y0s", out var offset));
            var baseTime = new DateTime(2026, 6, 15, 14, 30, 45, 123, DateTimeKind.Utc);
            var result = offset.Apply(baseTime);
            Assert.Equal(new DateTime(2033, 6, 15, 14, 30, 45, 0, DateTimeKind.Utc), result);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void Apply_NegativeOffset_SubtractsDays()
        {
            Assert.True(TimeFunctionOffset.TryParse("-1d", out var offset));
            var baseTime = new DateTime(2026, 3, 22, 14, 30, 45, DateTimeKind.Utc);
            var result = offset.Apply(baseTime);
            Assert.Equal(new DateTime(2026, 3, 21, 0, 0, 0, DateTimeKind.Utc), result);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void Apply_ZeroOffset_FloorsToUnit()
        {
            Assert.True(TimeFunctionOffset.TryParse("0h", out var offset));
            var baseTime = new DateTime(2026, 3, 22, 14, 30, 45, DateTimeKind.Utc);
            var result = offset.Apply(baseTime);
            Assert.Equal(new DateTime(2026, 3, 22, 14, 0, 0, DateTimeKind.Utc), result);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void Apply_ReadableUnitsWithSpaces()
        {
            Assert.True(TimeFunctionOffset.TryParse("+1 year 6 months", out var offset));
            var baseTime = new DateTime(2026, 1, 15, 14, 30, 45, DateTimeKind.Utc);
            var result = offset.Apply(baseTime);
            Assert.Equal(new DateTime(2027, 7, 1, 0, 0, 0, DateTimeKind.Utc), result);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void CanParseWithWhitespaceAfterSign()
        {
            Assert.True(TimeFunctionOffset.TryParse("+ 1d", out var result));
            Assert.Equal(1, result.Days);
            Assert.False(result.IsNegative);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void CanParseWithMultipleSpaces()
        {
            Assert.True(TimeFunctionOffset.TryParse("1y  6mo", out var result));
            Assert.Equal(1, result.Years);
            Assert.Equal(6, result.Months);
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void FailsOnWhitespaceOnly()
        {
            Assert.False(TimeFunctionOffset.TryParse("   ", out _));
        }
    }
}
