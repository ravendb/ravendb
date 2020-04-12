using System;
using System.Text;
using Sparrow.Json.Parsing;

namespace Sparrow
{
    public struct TimeValue : IDynamicJson, IEquatable<TimeValue>
    {
        private const int SecondsPerDay = 86_400;

        public static readonly TimeValue Zero = new TimeValue
        {
            Months = 0,
            Seconds = 0
        };

        public static readonly TimeValue MaxValue = new TimeValue
        {
            Months = int.MaxValue,
            Seconds = int.MaxValue
        };

        public static readonly TimeValue MinValue = new TimeValue
        {
            Months = int.MinValue, 
            Seconds = int.MinValue
        };

        public int Months { get; private set; }

        public int Seconds { get; private set; }

        private TimeValue(int months, int seconds)
        {
            Months = months;
            Seconds = seconds;
            AssertMonthOrSeconds();
        }

        public static TimeValue FromSeconds(int seconds)
        {
            return new TimeValue(0, seconds);
        }

        public static TimeValue FromMinutes(int minutes)
        {
            return new TimeValue(0, minutes * 60);
        }

        public static TimeValue FromHours(int hours)
        {
            return new TimeValue(0, hours * 3_600);
        }

        public static TimeValue FromDays(int days)
        {
            return new TimeValue(0, days * SecondsPerDay);
        }

        public static TimeValue FromMonths(int months)
        {
            return new TimeValue(months, 0);
        }

        public static TimeValue FromYears(int years)
        {
            return new TimeValue(years * 12, 0);
        }

        public long Ticks
        {
            get
            {
                AssertMonthOrSeconds();

                if (this == MaxValue)
                    return DateTime.MaxValue.Ticks;

                if (this == MinValue)
                    return DateTime.MinValue.Ticks;

                if (Seconds != 0)
                    return TimeSpan.FromSeconds(Seconds).Ticks;

                if (Months != 0)
                    return new DateTime(0, Months, 0).Ticks;

                return 0;
            }
        }

        public long TotalMilliseconds
        {
            get
            {
                if (this == MaxValue)
                    return (long)TimeSpan.MaxValue.TotalMilliseconds;

                if (this == MinValue)
                    return (long)TimeSpan.MinValue.TotalMilliseconds;

                AssertMonthIsZero();
                return Seconds * 1_000;
            }
        }

        public long TotalSeconds
        {
            get
            {
                if (this == MaxValue)
                    return (long)TimeSpan.MaxValue.TotalSeconds;

                if (this == MinValue)
                    return (long)TimeSpan.MinValue.TotalSeconds;

                AssertMonthIsZero();
                return Seconds;
            }
        }

        public long TotalMinutes
        {
            get
            {
                if (this == MaxValue)
                    return (long)TimeSpan.MaxValue.TotalMinutes;

                if (this == MinValue)
                    return (long)TimeSpan.MinValue.TotalMinutes;

                AssertMonthIsZero();
                return Seconds / 60;
            }
        }

        public long TotalHours
        {
            get
            {
                if (this == MaxValue)
                    return (long)TimeSpan.MaxValue.TotalHours;

                if (this == MinValue)
                    return (long)TimeSpan.MinValue.TotalMinutes;

                AssertMonthIsZero();
                return Seconds / 3_600;
            }
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Seconds)] = Seconds,
                [nameof(Months)] = Months
            };
        }

        public override string ToString()
        {
            if (this == MaxValue)
                return "MaxValue";
            if (this == MinValue)
                return "MinValue";
            if (this == Zero)
                return "Zero";

            var str = new StringBuilder();
            if (Months > 12)
                str.Append($"{Months / 12} years ");
            if (Months > 0)
                str.Append($"{Months % 12} months ");

            var remainingSeconds = Seconds;

            if (remainingSeconds > SecondsPerDay)
            {
                var days = Seconds / SecondsPerDay;
                str.Append($"{days} days ");
                remainingSeconds -= days * SecondsPerDay;
            }

            if (remainingSeconds > 3_600)
            {
                var hours = remainingSeconds / 3_600;
                str.Append($"{hours} hours ");
                remainingSeconds -= hours * 3_600;
            }

            if (remainingSeconds > 60)
            {
                var minutes = remainingSeconds / 60;
                str.Append($"{minutes} minutes ");
                remainingSeconds -= minutes * 60;
            }

            if (remainingSeconds > 0)
            {
                str.Append($"{remainingSeconds} seconds");
            }

            return str.ToString();
        }

        private void AssertMonthIsZero()
        {
            if (Months != 0)
                throw new ArgumentException("Must be zero", nameof(Months));
        }

        private void AssertMonthOrSeconds()
        {
            if (Months == 0 || Seconds == 0) 
                return;

            if (this == MaxValue || this == MinValue)
                return;

            throw new NotSupportedException($"Either {nameof(Months)} or {nameof(Seconds)} can be set.");
        }

        public int Compare(TimeValue other)
        {
            long monthsDiff = Months - other.Months;
            long secondsDiff = Seconds - other.Seconds;

            if (monthsDiff != 0 && secondsDiff != 0)
            {
                if ((monthsDiff ^ secondsDiff) < 0) // check for the same sign 
                    throw new InvalidOperationException();
            }

            var result = monthsDiff + secondsDiff;
            if (result > int.MaxValue)
                return int.MaxValue;

            if (result < int.MinValue)
                return int.MinValue;

            return (int)result;
        }

        public static TimeValue operator +(TimeValue a, TimeValue b)
        {
            return new TimeValue(a.Months + b.Months, a.Seconds + b.Seconds);
        }

        public static TimeValue operator -(TimeValue a, TimeValue b)
        {
            return new TimeValue(a.Months - b.Months, a.Seconds - b.Seconds);
        }

        public static TimeValue operator -(TimeValue a)
        {
            return new TimeValue(-a.Months, -a.Seconds);
        }

        public static TimeValue operator *(TimeValue a, double c)
        {
            return new TimeValue((int)(c * a.Months), (int)(c * a.Seconds));
        }

        public static bool operator >(TimeValue a, TimeValue b)
        {
            return a.Compare(b) > 0;
        }

        public static bool operator <(TimeValue a, TimeValue b)
        {
            return a.Compare(b) < 0;
        }

        public static bool operator >=(TimeValue a, TimeValue b)
        {
            return a.Compare(b) >= 0;
        }

        public static bool operator <=(TimeValue a, TimeValue b)
        {
            return a.Compare(b) <= 0;
        }

        public static bool operator ==(TimeValue a, TimeValue b)
        {
            return a.Compare(b) == 0;
        }

        public static bool operator !=(TimeValue a, TimeValue b)
        {
            return !(a == b);
        }

        public static implicit operator TimeSpan(TimeValue a)
        {
            a.AssertMonthIsZero();
            return new TimeSpan(0, 0, a.Seconds);
        }

        public bool Equals(TimeValue other)
        {
            return Compare(other) == 0;
        }

        public override bool Equals(object obj)
        {
            return obj is TimeValue other && Equals(other);
        }

        public override int GetHashCode()
        {
            return Hashing.Combine(Months, Seconds);
        }
    }

    public static class DateTimeExtenstion
    {
        public static DateTime Add(this DateTime date, TimeValue time)
        {
            return date.AddMonths(time.Months).AddSeconds(time.Seconds);
        }
    }
}
