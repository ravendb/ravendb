using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using NCrontab.Advanced;
using NCrontab.Advanced.Enumerations;
using NCrontab.Advanced.Filters;

namespace Raven.Server.Utils
{
    /// <summary>
    /// Extension methods for CrontabSchedule to provide additional functionality
    /// </summary>
    public static class CrontabScheduleExtensions
    {
        /// <summary>
        /// Gets the previous occurrence of the cron schedule before or at the specified base value, but not before the specified start value
        /// </summary>
        /// <param name="schedule">The cron schedule</param>
        /// <param name="baseValue">The base date/time to search backwards from</param>
        /// <param name="startValue">The earliest date/time to consider for the search. If not specified, defaults to 4 years before the base value.</param>
        /// <returns>The previous occurrence before or at the base value but not before start value</returns>
        public static DateTime? GetPreviousOccurrence(this CrontabSchedule schedule, DateTime baseValue, DateTime startValue = default)
        {
            ArgumentNullException.ThrowIfNull(schedule);
            return InternalGetPreviousOccurrence(schedule, baseValue, startValue);
        }

        /// <summary>
        /// Gets multiple previous occurrences of the cron schedule
        /// </summary>
        /// <param name="schedule">The cron schedule</param>
        /// <param name="baseTime">The base date/time to search backwards from</param>
        /// <param name="startTime">The earliest date/time to consider. If not specified, defaults to 4 years before the base time.</param>
        /// <returns>An enumerable of previous occurrences</returns>
        public static IEnumerable<DateTime?> GetPreviousOccurrences(this CrontabSchedule schedule, DateTime baseTime, DateTime startTime = default)
        {
            ArgumentNullException.ThrowIfNull(schedule);

            for (var occurrence = GetPreviousOccurrence(schedule, baseTime, startTime);
                 occurrence > startTime;
                 occurrence = GetPreviousOccurrence(schedule, occurrence.Value.AddMinutes(-1), startTime))
            {
                yield return occurrence;
            }
        }

        private static DateTime? InternalGetPreviousOccurrence(CrontabSchedule schedule, DateTime baseValue, DateTime startValue = default)
        {
            if (startValue == default)
                startValue = baseValue.AddYears(-4); // Default to 4 years before baseValue if not specified

            var targetKind = baseValue.Kind == DateTimeKind.Unspecified
                ? DateTimeKind.Utc
                : baseValue.Kind;

            var newValue = new DateTime(baseValue.Year, baseValue.Month, baseValue.Day, baseValue.Hour, baseValue.Minute, 0, targetKind);

            if (newValue < startValue)
                return null;

            var timeZone = baseValue.Kind == DateTimeKind.Utc
                ? TimeZoneInfo.Utc
                : TimeZoneInfo.Local;

            var minuteValues = CrontabScheduleCache.GetOrAddValidValues(schedule, CrontabFieldKind.Minute);
            var hourValues = CrontabScheduleCache.GetOrAddValidValues(schedule, CrontabFieldKind.Hour);
            var dayValues = CrontabScheduleCache.GetOrAddValidValues(schedule, CrontabFieldKind.Day);
            var monthValues = CrontabScheduleCache.GetOrAddValidValues(schedule, CrontabFieldKind.Month);

            var currentYear = newValue.Year;
            while (currentYear >= startValue.Year)
            {
                var startMonth = currentYear == newValue.Year
                    ? newValue.Month
                    : Constants.MaximumDateTimeValues[CrontabFieldKind.Month];

                for (var month = startMonth; month >= 1; month--)
                {
                    if (currentYear == startValue.Year && month < startValue.Month)
                        break;

                    if (monthValues.Contains(month) == false)
                        continue;

                    var daysInMonth = DateTime.DaysInMonth(currentYear, month);
                    var startDay = currentYear == newValue.Year && month == newValue.Month
                        ? newValue.Day
                        : daysInMonth;

                    for (var day = startDay; day >= 1; day--)
                    {
                        if (currentYear == startValue.Year && month == startValue.Month && day < startValue.Day)
                            break;

                        if (dayValues.Contains(day) == false)
                            continue;

                        var candidateDate = new DateTime(currentYear, month, day);
                        if (schedule.IsMatch(candidateDate, CrontabFieldKind.DayOfWeek) == false)
                            continue;

                        var startHour = currentYear == newValue.Year && month == newValue.Month && day == newValue.Day
                            ? newValue.Hour
                            : Constants.MaximumDateTimeValues[CrontabFieldKind.Hour];

                        for (var hour = startHour; hour >= 0; hour--)
                        {
                            if (currentYear == startValue.Year && month == startValue.Month && day == startValue.Day && hour < startValue.Hour)
                                break;

                            if (hourValues.Contains(hour) == false)
                                continue;

                            var startMinute = currentYear == newValue.Year && month == newValue.Month && day == newValue.Day && hour == newValue.Hour
                                ? newValue.Minute
                                : Constants.MaximumDateTimeValues[CrontabFieldKind.Minute];

                            for (var minute = startMinute; minute >= 0; minute--)
                            {
                                if (currentYear == startValue.Year && month == startValue.Month && day == startValue.Day && hour == startValue.Hour && minute < startValue.Minute)
                                    break;

                                if (minuteValues.Contains(minute) == false)
                                    continue;

                                var finalCandidate = new DateTime(currentYear, month, day, hour, minute, 0, newValue.Kind);

                                if (timeZone.IsInvalidTime(finalCandidate) == false && schedule.IsMatch(finalCandidate))
                                    return finalCandidate;
                            }
                        }
                    }
                }
                currentYear--;
            }

            return null;
        }

        internal static class CrontabScheduleCache
        {
            private static readonly ConcurrentDictionary<string, ConcurrentDictionary<CrontabFieldKind, List<int>>> ValidValuesCache = new();

            public static List<int> GetOrAddValidValues(CrontabSchedule schedule, CrontabFieldKind kind)
            {
                var cronStringKey = schedule.ToString();

                var scheduleCache = ValidValuesCache.GetOrAdd(cronStringKey, _ => new ConcurrentDictionary<CrontabFieldKind, List<int>>());

                return scheduleCache.GetOrAdd(kind, static (crontabFieldKind, crontabSchedule) => GetValidValues(crontabSchedule, crontabFieldKind), schedule);
            }

            /// <summary>
            /// Gets valid values for a specific cron field by examining the filters
            /// </summary>
            private static List<int> GetValidValues(CrontabSchedule schedule, CrontabFieldKind kind)
            {
                var values = new HashSet<int>();
                var maxValue = Constants.MaximumDateTimeValues[kind];
                var minValue = Constants.MinimumDateTimeValues[kind];

                foreach (var filter in schedule.Filters[kind])
                {
                    switch (filter)
                    {
                        case AnyFilter _:
                            // For AnyFilter, return all possible values for this field efficiently
                            for (int i = minValue; i <= maxValue; i++)
                                values.Add(i);

                            return values.OrderBy(v => v).ToList(); // Early return for efficiency

                        case SpecificFilter specificFilter:
                            values.Add(specificFilter.SpecificValue);

                            continue;

                        case RangeFilter rangeFilter:
                            foreach (var rangeSpecificFilter in rangeFilter.SpecificFilters)
                                values.Add(rangeSpecificFilter.SpecificValue);

                            continue;

                        case StepFilter stepFilter:
                            foreach (var stepSpecificFilter in stepFilter.SpecificFilters)
                                values.Add(stepSpecificFilter.SpecificValue);

                            continue;

                        case BlankDayOfMonthOrWeekFilter _:
                            for (int i = minValue; i <= maxValue; i++)
                                values.Add(i);

                            continue;

                        case LastDayOfMonthFilter _:
                            for (int day = 28; day <= 31; day++) // The last days can be 28, 29, 30, or 31
                                values.Add(day);

                            continue;

                        case LastWeekdayOfMonthFilter _:
                            for (int day = 26; day <= 31; day++) // Conservative range
                                values.Add(day);

                            continue;

                        case NearestWeekdayFilter nearestWeekdayFilter:
                            var targetDay = nearestWeekdayFilter.SpecificValue;
                            var minNearestWeekday = Math.Max(1, targetDay - 2);
                            var maxNearestWeekday = Math.Min(31, targetDay + 2);

                            for (int day = minNearestWeekday; day <= maxNearestWeekday; day++)
                                values.Add(day);

                            continue;

                        case SpecificDayOfWeekInMonthFilter specificDayOfWeekFilter:
                            var weekNumber = specificDayOfWeekFilter.WeekNumber;
                            var minSpecificDayOfWeek = Math.Max(1, (weekNumber - 1) * 7 - 6); // Conservative lower bound
                            var maxSpecificDayOfWeek = Math.Min(31, weekNumber * 7 + 6);      // Conservative upper bound

                            for (int day = minSpecificDayOfWeek; day <= maxSpecificDayOfWeek; day++)
                                values.Add(day);

                            continue;

                        case LastDayOfWeekInMonthFilter _:
                            for (int day = 15; day <= 31; day++) // Last 2+ weeks
                                values.Add(day);

                            continue;
                    }
                }

                return values.OrderBy(v => v).ToList();
            }
        }
    }
}
