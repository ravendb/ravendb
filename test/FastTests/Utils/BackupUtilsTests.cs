using System;
using NCrontab.Advanced;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Utils
{
    public class BackupUtilsTest(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenTheory(RavenTestCategory.BackupExportImport)]
        // Minute-based Schedules (every 15 minutes)
        [InlineData("*/15 * * * *", "2023-10-30 10:15:00", "2023-10-30 10:15:00")] // Base time is exactly on the scheduled time
        [InlineData("*/15 * * * *", "2023-10-30 10:20:00", "2023-10-30 10:15:00")] // Base time is between occurrences
        [InlineData("*/15 * * * *", "2023-10-30 10:14:59", "2023-10-30 10:00:00")] // Base time is just before a scheduled time

        // Hourly Schedules
        [InlineData("0 * * * *", "2023-10-30 14:00:00", "2023-10-30 14:00:00")]   // Exactly on the hour
        [InlineData("0 * * * *", "2023-10-30 14:30:15", "2023-10-30 14:00:00")]   // In the middle of the hour

        // Daily Schedules
        [InlineData("30 8 * * *", "2023-10-28 08:30:00", "2023-10-28 08:30:00")] // Exactly on the daily schedule
        [InlineData("30 8 * * *", "2023-10-28 08:29:59", "2023-10-27 08:30:00")] // Just before the daily schedule (crosses day boundary)

        // Weekly Schedules (every Monday at 9:00 AM)
        [InlineData("0 9 * * 1", "2023-10-30 09:00:00", "2023-10-30 09:00:00")] // Exactly on the scheduled time
        [InlineData("0 9 * * 1", "2023-10-30 08:59:00", "2023-10-23 09:00:00")] // Just before the scheduled time (crosses week boundary)

        // Monthly Schedules (1st of the month at midnight)
        [InlineData("0 0 1 * *", "2023-11-15 12:00:00", "2023-11-01 00:00:00")] // Middle of the month
        [InlineData("0 0 1 * *", "2023-10-31 23:59:59", "2023-10-01 00:00:00")] // Just before the scheduled time (crosses month boundary)

        // Quarterly Schedules (15th of March, June, September, December at midnight)
        [InlineData("0 0 15 */3 *", "2024-06-15 00:00:00", "2024-06-15 00:00:00")] // Exactly on the scheduled time
        [InlineData("0 0 15 */3 *", "2024-05-20 10:00:00", "2024-03-15 00:00:00")] // Middle of May, the last occurrence is in March

        // Yearly Schedules (January 1st at midnight)
        [InlineData("0 0 1 1 *", "2024-01-01 00:00:00", "2024-01-01 00:00:00")] // Exactly on the scheduled time
        [InlineData("0 0 1 1 *", "2023-12-31 23:59:59", "2023-01-01 00:00:00")] // Just before the scheduled time (crosses year boundary)

        // Schedule for Feb 29, at noon
        [InlineData("0 12 29 2 *", "2024-03-01 10:00:00", "2024-02-29 12:00:00")] // From 2024 (leap) looking back
        [InlineData("0 12 29 2 *", "2025-03-01 10:00:00", "2024-02-29 12:00:00")] // From 2025 (non-leap) looking back to the last leap year
        public void GetLastOccurrence_WithStandardSchedules_ShouldReturnCorrectPastDateTime(string cronExpression, string baseTimeString, string expectedTimeString)
        {
            // Arrange
            var schedule = CrontabSchedule.Parse(cronExpression);
            var baseTime = DateTime.Parse(baseTimeString);
            var expected = DateTime.Parse(expectedTimeString);

            // Act
            var actual = BackupUtils.GetLastOccurrence(schedule, baseTime);

            // Assert
            Assert.NotNull(actual);
            Assert.Equal(expected, actual.Value);
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public void GetLastOccurrence_WhenBaseTimeIsBeforeSupportedDate_ShouldThrowException()
        {
            // Arrange
            var schedule = CrontabSchedule.Parse("0 0 * * *");
            var minSupportedDate = BackupUtils.BackupScheduleSearchLowerBound;
            var invalidBaseTime = minSupportedDate.AddTicks(-1);

            // Act & Assert
            var ex = Assert.Throws<ArgumentOutOfRangeException>(() => BackupUtils.GetLastOccurrence(schedule, invalidBaseTime));
            Assert.Contains("Dates before 2015-01-01 are not supported", ex.Message);
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public void GetLastOccurrence_WhenBaseTimeIsAtSupportedDateBoundary_ShouldWork()
        {
            var schedule = CrontabSchedule.Parse("0 0 1 * *");

            // Act: Base time is just after the first possible occurrence within the supported range.
            var baseTime = new DateTime(2015, 1, 15, 12, 0, 0);
            var expected = new DateTime(2015, 1, 1, 0, 0, 0);
            var actual = BackupUtils.GetLastOccurrence(schedule, baseTime);

            // Assert
            Assert.NotNull(actual);
            Assert.Equal(expected, actual.Value);
        }
    }
}
