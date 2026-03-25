using System;
using NCrontab.Advanced;
using NCrontab.Advanced.Exceptions;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Utils
{
    public class BackupUtilsTest(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenTheory(RavenTestCategory.BackupExportImport)]

        #region Minute-based Schedules
        [InlineData("* * * * *", "2023-10-30 10:00:00", "2023-10-30 10:00:00")]           // Every minute
        [InlineData("*/15 * * * *", "2023-10-30 10:15:00", "2023-10-30 10:15:00")]        // Base time is exactly on the scheduled time
        [InlineData("*/15 * * * *", "2023-10-30 10:20:00", "2023-10-30 10:15:00")]        // Base time is between occurrences
        [InlineData("*/15 * * * *", "2023-10-30 10:14:59", "2023-10-30 10:00:00")]        // Base time is just before a scheduled time
        #endregion

        #region Hourly Schedules
        [InlineData("0 * * * *", "2023-10-30 14:00:00", "2023-10-30 14:00:00")]           // Exactly on the hour
        [InlineData("0 * * * *", "2023-10-30 14:30:15", "2023-10-30 14:00:00")]           // In the middle of the hour
        #endregion

        #region Daily Schedules
        [InlineData("30 8 * * *", "2023-10-28 08:30:00", "2023-10-28 08:30:00")]          // Exactly on the daily schedule
        [InlineData("30 8 * * *", "2023-10-28 08:29:59", "2023-10-27 08:30:00")]          // Just before the daily schedule (crosses day boundary)
        #endregion

        #region Weekly Schedules
        [InlineData("0 9 * * 1", "2023-10-30 09:00:00", "2023-10-30 09:00:00")]           // Exactly on the scheduled time (Monday)
        [InlineData("0 9 * * 1", "2023-10-30 08:59:00", "2023-10-23 09:00:00")]           // Just before the scheduled time (crosses week boundary)
        [InlineData("0 0 * * 0", "2023-10-30 10:00:00", "2023-10-29 00:00:00")]           // Every Sunday
        #endregion

        #region Monthly Schedules
        [InlineData("0 0 1 * *", "2023-11-15 12:00:00", "2023-11-01 00:00:00")]           // Middle of the month
        [InlineData("0 0 1 * *", "2023-10-31 23:59:59", "2023-10-01 00:00:00")]           // Just before the scheduled time (crosses month boundary)
        [InlineData("0 9 1,15 * *", "2023-10-30 10:00:00", "2023-10-15 09:00:00")]        // 1st and 15th of month
        #endregion

        #region Yearly and Quarterly Schedules
        [InlineData("0 0 15 */3 *", "2024-06-15 00:00:00", "2024-06-15 00:00:00")]        // Quarterly: Exactly on the scheduled time
        [InlineData("0 0 15 */3 *", "2024-05-20 10:00:00", "2024-03-15 00:00:00")]        // Quarterly: Middle of May, the last occurrence is in March
        [InlineData("0 0 1 1 *", "2024-01-01 00:00:00", "2024-01-01 00:00:00")]           // Yearly: Exactly on the scheduled time
        [InlineData("0 0 1 1 *", "2023-12-31 23:59:59", "2023-01-01 00:00:00")]           // Yearly: Just before the scheduled time (crosses year boundary)
        #endregion

        #region Special Characters (L, W, #) and Ranges
        [InlineData("0 0 L * *", "2024-03-15 10:00:00", "2024-02-29 00:00:00")]           // Last day of month (Feb, leap year)
        [InlineData("0 0 L * *", "2023-01-01 00:00:00", "2022-12-31 00:00:00")]           // Last day of previous year
        [InlineData("0 0 * * 5L", "2024-06-10 12:00:00", "2024-05-31 00:00:00")]          // Last Friday of May
        [InlineData("0 0 * * 5L", "2023-11-01 00:00:00", "2023-10-27 00:00:00")]          // Last Friday of October
        [InlineData("0 0 LW * *", "2023-11-01 00:00:00", "2023-10-31 00:00:00")]          // Last weekday of month
        [InlineData("0 0 15W * *", "2023-10-30 10:00:00", "2023-10-16 00:00:00")]         // Nearest weekday to 15th of Oct (15th is Sunday, so it's Monday 16th)
        #endregion

        #region Step Values (/) and Complex Ranges
        [InlineData("*/7 */3 * * *", "2023-10-30 15:34:00", "2023-10-30 15:28:00")]       // Every 7 minutes, every 3 hours
        [InlineData("*/7 */3 * * *", "2023-10-30 16:35:00", "2023-10-30 15:56:00")]       // Every 7 minutes, every 3 hours
        [InlineData("*/7 */3 * * *", "2023-10-30 15:35:00", "2023-10-30 15:35:00")]       // Every 7 minutes, every 3 hours
        [InlineData("15/20 * * * *", "2023-10-30 14:40:00", "2023-10-30 14:35:00")]       // Minutes 15, 35, 55
        [InlineData("* 2/8 * * *", "2023-10-30 11:00:00", "2023-10-30 10:59:00")]         // Hours 2, 10, 18
        [InlineData("0-30/10 * * * *", "2023-10-30 14:25:00", "2023-10-30 14:20:00")]     // Minutes 0, 10, 20, 30
        [InlineData("* 9-17/2 * * *", "2023-10-30 16:30:00", "2023-10-30 15:59:00")]      // Hours 9, 11, 13, 15, 17
        [InlineData("0,30 8-18/2 * * 1-5", "2023-10-30 11:15:00", "2023-10-30 10:30:00")] // Complex: weekdays, specific hours and minutes
        #endregion

        #region Day of Week (Lists and Ranges)
        [InlineData("0 9 * * 1-5", "2023-10-30 10:00:00", "2023-10-30 09:00:00")]         // Mon-Fri at 9:00 (Oct 30 is Monday)
        [InlineData("0 9 * * 1-5", "2023-10-29 10:00:00", "2023-10-27 09:00:00")]         // Mon-Fri at 9:00 (Oct 29 is Sunday, last occurrence was Friday)
        [InlineData("0 9 * * 1,3,5", "2023-10-30 10:00:00", "2023-10-30 09:00:00")]       // Mon,Wed,Fri at 9:00
        [InlineData("0 9 * * 6,0", "2023-10-30 10:00:00", "2023-10-29 09:00:00")]         // Sat,Sun at 9:00
        #endregion

        #region Edge Cases: Leap Years, DST, Boundaries
        [InlineData("0 12 29 2 *", "2024-03-01 10:00:00", "2024-02-29 12:00:00")]         // Leap day from leap year
        [InlineData("0 12 29 2 *", "2025-03-01 10:00:00", "2024-02-29 12:00:00")]         // Leap day from non-leap year (find previous)
        [InlineData("0 12 29 2 *", "2027-03-01 10:00:00", "2024-02-29 12:00:00")]         // Leap day, looking back across multiple years
        [InlineData("0 12 30 2 *", "2025-03-01 10:00:00", "2025-03-01 10:00:00")]         // Non-existent date - should throw an exception
        [InlineData("30 1 * * *", "2024-10-27 08:00:00", "2024-10-27 01:30:00")]          // DST transition (Israel Standard Time)
        [InlineData("0 3 * * *", "2024-03-29 08:00:00", "2024-03-29 03:00:00")]           // Hour after spring-forward DST transition
        [InlineData("0 1 * * *", "2024-03-29 08:00:00", "2024-03-29 01:00:00")]           // Hour before spring-forward DST transition
        [InlineData("30 14 * * *", "2023-10-30 14:30:30.500", "2023-10-30 14:30:00")]     // Seconds/milliseconds should be trimmed
        [InlineData("0 0 1 1 *", "2023-01-01 00:00:01", "2023-01-01 00:00:00")]           // Boundary: New Year one second later
        [InlineData("59 23 31 12 *", "2024-01-01 00:00:00", "2023-12-31 23:59:00")]       // Boundary: End of year
        #endregion

        #region Edge Cases for Context-Dependent Filters
        [InlineData("0 0 1W * *", "2025-03-15 00:00:00", "2025-03-03 00:00:00")]          // Mar 1, 2025 is a Saturday. 1W -> Mon, Mar 3.
        [InlineData("0 0 31W * *", "2025-09-01 00:00:00", "2025-08-29 00:00:00")]         // Aug 31, 2025 is a Sunday. 31W -> Fri, Aug 29.
        [InlineData("0 0 * * 1#1", "2025-07-19 00:00:00", "2025-07-07 00:00:00")]         // Jul 1, 2023 is Tuesday. First Mon is Jul 7.
        [InlineData("0 0 * * 2#5", "2025-07-31 00:00:00", "2025-07-29 00:00:00")]         // July 2025 has a 5th Tuesday on the 29th.
        [InlineData("0 0 * * 3#5", "2025-02-28 00:00:00", "2025-01-29 00:00:00")]         // Feb 2025 has no 5th Wed. Prev month (Jan) has one on 29st.
        [InlineData("0 0 * * 3L", "2025-07-19 00:00:00", "2025-06-25 00:00:00")]          // Last Wed in Jun 2025 is the 25th.
        [InlineData("0 0 * * 1L", "2025-01-01 00:00:00", "2024-12-30 00:00:00")]          // Last Mon of 2024 is Dec 30th.
        #endregion

        public void GetPreviousOccurrence_WithVariousSchedules_ShouldReturnCorrectPastDateTime(string cronExpression, string baseTimeString, string expectedTimeString)
        {
            // Arrange
            var baseTime = DateTime.Parse(baseTimeString);
            var expected = DateTime.Parse(expectedTimeString);

            CrontabSchedule schedule = null;
            var exception = Record.Exception(() => schedule = CrontabSchedule.Parse(cronExpression));
            if (exception != null)
            {
                Assert.IsType<CrontabException>(exception);
                return;
            }

            // Act
            var actual = schedule.GetPreviousOccurrence(baseTime);

            // Assert
            Assert.Equal(expected, actual);
        }
    }
}
