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

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Seconds)] = Seconds,
                [nameof(Months)] = Months
            };
        }

        private void Append(StringBuilder builder, int value, string singular)
        {
            if (value <= 0)
                return;

            builder.Append(value);
            builder.Append(' ');
            builder.Append(singular);

            if (value == 1)
            {
                builder.Append(' ');
                return;
            }

            builder.Append("s "); // lucky me, no special rules here
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
            if (Months >= 12)
                Append(str, Months / 12, "year");
            if (Months % 12 > 0)
                Append(str, Months % 12, "month");

            var remainingSeconds = Seconds;

            if (remainingSeconds > SecondsPerDay)
            {
                var days = Seconds / SecondsPerDay;
                Append(str, days, "day");
                remainingSeconds -= days * SecondsPerDay;
            }

            if (remainingSeconds > 3_600)
            {
                var hours = remainingSeconds / 3_600;
                Append(str, hours, "hour");
                remainingSeconds -= hours * 3_600;
            }

            if (remainingSeconds > 60)
            {
                var minutes = remainingSeconds / 60;
                Append(str, minutes, "minute");
                remainingSeconds -= minutes * 60;
            }

            if (remainingSeconds > 0)
                Append(str, remainingSeconds, "second");

            return str.ToString();
        }

        private void AssertMonthIsZero()
        {
            if (Months != 0)
                throw new ArgumentException("Must be zero", nameof(Months));
        }

        internal void AssertMonthOrSeconds()
        {
            if (Months == 0 || Seconds == 0) 
                return;

            if (this == MaxValue || this == MinValue)
                return;

            throw new NotSupportedException($"Either {nameof(Months)} or {nameof(Seconds)} can be set.");
        }

        private const int SecondsIn28Days = 28 * SecondsPerDay; // lower-bound of seconds in month
        private const int SecondsIn31Days = 31 * SecondsPerDay; // upper-bound of seconds in month

        public int Compare(TimeValue other)
        {
            if (IsSpecialCompare(ref this, ref other, out var result))
                return result;

            if (Seconds == other.Seconds)
                return TrimCompareResult(Months - other.Months);

            if (Months == other.Months) 
                return TrimCompareResult(Seconds - other.Seconds);

            var myBounds = GetBounds(this);
            var otherBounds = GetBounds(other);

            if (otherBounds.UpperBound < myBounds.LowerBound)
                return 1;

            if (otherBounds.LowerBound > myBounds.UpperBound)
                return -1;
            
            throw new InvalidOperationException("We can't compare ");
        }

        private static (int UpperBound, int LowerBound) GetBounds(TimeValue time)
        {
            var myUpperBound = time.Months * SecondsIn31Days + time.Seconds;
            var myLowerBound = time.Months * SecondsIn28Days + time.Seconds;
            return (myUpperBound, myLowerBound);
        }

        private static bool IsSpecialCompare(ref TimeValue current, ref TimeValue other, out int result)
        {
            result = 0;
            if (IsMax(ref current))
            {
                result = IsMax(ref other) ? 0 : 1;
                return true;
            }

            if (IsMax(ref other))
            {
                result = IsMax(ref current) ? 0 : -1;
                return true;
            }

            if (IsMin(ref current))
            {
                result = IsMin(ref other) ? 0 : -1;
                return true;
            }

            if (IsMin(ref other))
            {
                result = IsMin(ref current) ? 0 : 1;
                return true;
            }

            return false;
        }

        private static bool IsMax(ref TimeValue time)
        {
            return time.Seconds == int.MaxValue && time.Months == int.MaxValue;
        }

        private static bool IsMin(ref TimeValue time)
        {
            return time.Seconds == int.MinValue && time.Months == int.MinValue;
        }

        private static int TrimCompareResult(long result)
        {
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

        public static explicit operator TimeSpan(TimeValue a)
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
