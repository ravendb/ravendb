using System;
using System.Text;
using Sparrow.Json.Parsing;

namespace Sparrow
{
    public enum TimeValueUnit
    {
        None,
        Second,
        Month
    }
    public struct TimeValue : IDynamicJson, IEquatable<TimeValue>
    {
        public static readonly TimeValue Zero = new TimeValue
        {
            Value = 0
        };

        public static readonly TimeValue MaxValue = new TimeValue
        {
            Value = int.MaxValue
        };

        public static readonly TimeValue MinValue = new TimeValue
        {
            Value = int.MinValue
        };

        public int Value { get; private set; }
        public TimeValueUnit Unit { get; private set; }

        private TimeValue(int value, TimeValueUnit unit)
        {
            AssertValidUnit(unit);

            Value = value;
            Unit = unit;
        }

        public static TimeValue FromSeconds(int seconds)
        {
            return new TimeValue(seconds, TimeValueUnit.Second);
        }

        public static TimeValue FromMinutes(int minutes)
        {
            return new TimeValue(minutes * 60, TimeValueUnit.Second);
        }

        public static TimeValue FromHours(int hours)
        {
            return new TimeValue(hours * 3600, TimeValueUnit.Second);
        }

        public static TimeValue FromDays(int days)
        {
            return new TimeValue(days * SecondsPerDay, TimeValueUnit.Second);
        }

        public static TimeValue FromMonths(int months)
        {
            return new TimeValue(months, TimeValueUnit.Month);
        }

        public static TimeValue FromYears(int years)
        {
            return new TimeValue(12 * years, TimeValueUnit.Month);
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Value)] = Value,
                [nameof(Unit)] = Unit
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
            if (Value == int.MaxValue)
                return "MaxValue";
            if (Value == int.MinValue)
                return "MinValue";
            if (Value == 0)
                return "Zero";

            if (Unit == TimeValueUnit.None)
                return "Unknown time unit";

            var str = new StringBuilder();
            switch (Unit)
            {
                case TimeValueUnit.Second:
                    var remainingSeconds = Value;

                    if (remainingSeconds > SecondsPerDay)
                    {
                        var days = Value / SecondsPerDay;
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
                    break;

                case TimeValueUnit.Month:
                    if (Value >= 12)
                        Append(str, Value / 12, "year");
                    if (Value % 12 > 0)
                        Append(str, Value % 12, "month");
                    break;
                
                default:
                    throw new ArgumentOutOfRangeException(nameof(Unit), $"Not supported time value unit '{Unit}'");
            }

            return str.ToString().TrimEnd();
        }

        private void AssertSeconds()
        {
            if (Unit != TimeValueUnit.Second)
                throw new ArgumentException("The value must be in seconds", nameof(Unit));
        }


        private static void AssertValidUnit(TimeValueUnit unit)
        {
            if (unit == TimeValueUnit.Month || unit == TimeValueUnit.Second)
                return;

            throw new ArgumentException($"Invalid time unit {unit}");
        }

        private static void AssertSameUnits(TimeValue a, TimeValue b)
        {
            if (a.Unit != b.Unit)
                throw new InvalidOperationException($"Unit isn't the same {a.Unit} != {b.Unit}");
        }

        private const int SecondsPerDay = 86_400;
        private const int SecondsIn28Days = 28 * SecondsPerDay; // lower-bound of seconds in month
        private const int SecondsIn31Days = 31 * SecondsPerDay; // upper-bound of seconds in month
        private const int SecondsIn365Days = 365 * SecondsPerDay; // lower-bound of seconds in a year
        private const int SecondsIn366Days = 366 * SecondsPerDay; // upper-bound of seconds in a year

        public int Compare(TimeValue other)
        {
            if (Value == 0 || other.Value == 0)
                return Value - other.Value;

            if (IsSpecialCompare(this, other, out var result))
                return result;

            if (Unit == other.Unit)
                return TrimCompareResult(Value - other.Value);

            var myBounds = GetBoundsInSeconds(this);
            var otherBounds = GetBoundsInSeconds(other);

            if (otherBounds.UpperBound < myBounds.LowerBound)
                return 1;

            if (otherBounds.LowerBound > myBounds.UpperBound)
                return -1;
            
            throw new InvalidOperationException($"Unable to compare {this} with {other}, since a month might have different number of days.");
        }

        private static (long UpperBound, long LowerBound) GetBoundsInSeconds(TimeValue time)
        {
            switch (time.Unit)
            {
                case TimeValueUnit.Second:
                    return (time.Value, time.Value);

                case TimeValueUnit.Month:
                    var years = time.Value / 12;
                    var upperBound = years * SecondsIn366Days;
                    var lowerBound = years * SecondsIn365Days;

                    var remainingMonths = time.Value % 12;
                    upperBound += remainingMonths * SecondsIn31Days;
                    lowerBound += remainingMonths * SecondsIn28Days;
                    return (upperBound, lowerBound);
         
                default:
                    throw new ArgumentOutOfRangeException(nameof(time.Unit), $"Not supported time value unit '{time.Unit}'");
            }
        }

        private static bool IsSpecialCompare(TimeValue current, TimeValue other, out int result)
        {
            result = 0;
            if (IsMax(current))
            {
                result = IsMax(other) ? 0 : 1;
                return true;
            }

            if (IsMax(other))
            {
                result = IsMax(current) ? 0 : -1;
                return true;
            }

            if (IsMin(current))
            {
                result = IsMin(other) ? 0 : -1;
                return true;
            }

            if (IsMin(other))
            {
                result = IsMin(current) ? 0 : 1;
                return true;
            }

            return false;
        }

        private static bool IsMax(TimeValue time)
        {
            return time.Unit == TimeValueUnit.None && time.Value == int.MaxValue;
        }

        private static bool IsMin(TimeValue time)
        {
            return time.Unit == TimeValueUnit.None && time.Value == int.MinValue;
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
            if (a.Value == 0)
                return b;

            if (b.Value == 0)
                return a;

            AssertSameUnits(a, b);
            return new TimeValue(checked(a.Value + b.Value), a.Unit);
        }

        public static TimeValue operator -(TimeValue a, TimeValue b)
        {
            if (a.Value == 0)
                return -b;

            if (b.Value == 0)
                return a;

            AssertSameUnits(a, b);
            return new TimeValue(checked(a.Value - b.Value), a.Unit);
        }

        public static TimeValue operator -(TimeValue a)
        {
            return new TimeValue(-a.Value, a.Unit);
        }

        public static TimeValue operator *(TimeValue a, double c)
        {
            return new TimeValue(checked((int)(c * a.Value)), a.Unit);
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
            a.AssertSeconds();
            return new TimeSpan(0, 0, a.Value);
        }

        public static implicit operator TimeValue(TimeSpan a)
        {
            return new TimeValue(checked((int)a.TotalSeconds), TimeValueUnit.Second);
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
            return Hashing.Combine(Value, (int)Unit);
        }
    }

    public static class DateTimeExtenstion
    {
        public static DateTime Add(this DateTime date, TimeValue time)
        {
            if (time.Value == 0)
                return date;
            if (time.Value == int.MaxValue)
                return DateTime.MaxValue;
            if (time.Value == int.MinValue)
                return DateTime.MinValue;

            switch (time.Unit)
            {
                case TimeValueUnit.Month:
                    return date.AddMonths(time.Value);
                case TimeValueUnit.Second:
                    return date.AddSeconds(time.Value);
                default:
                    throw new ArgumentOutOfRangeException(nameof(time.Unit), $"Not supported time value unit '{time.Unit}'");
            }
        }
    }
}
