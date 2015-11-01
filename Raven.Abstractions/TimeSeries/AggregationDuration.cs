using System;

namespace Raven.Abstractions.TimeSeries
{
    public class AggregationDuration
    {
        public AggregationDuration(AggregationDurationType type, int duration)
        {
            Type = type;
            Duration = duration;
        }

        public AggregationDurationType Type { get; private set; }
        
        public int Duration { get; private set; }

        public static AggregationDuration Seconds(int duration)
        {
            return new AggregationDuration(AggregationDurationType.Seconds, duration);
        }

        public static AggregationDuration Minutes(int duration)
        {
            return new AggregationDuration(AggregationDurationType.Minutes, duration);
        }

        public static AggregationDuration Hours(int duration)
        {
            return new AggregationDuration(AggregationDurationType.Hours, duration);
        }

        public static AggregationDuration Days(int duration)
        {
            return new AggregationDuration(AggregationDurationType.Days, duration);
        }

        public static AggregationDuration Months(int duration)
        {
            return new AggregationDuration(AggregationDurationType.Months, duration);
        }

        public static AggregationDuration Years(int duration)
        {
            return new AggregationDuration(AggregationDurationType.Years, duration);
        }

        public override bool Equals(object obj)
        {
            var other = obj as AggregationDuration;
            if (other == null)
                return false;

            return Type == other.Type &&
                   Duration == other.Duration;
        }

        public override int GetHashCode()
        {
            int hashCode = Type.GetHashCode();
            hashCode = (hashCode * 397) ^ Duration.GetHashCode();
            return hashCode;
        }

        public DateTimeOffset AddToDateTime(DateTimeOffset start)
        {
            switch (Type)
            {
                case AggregationDurationType.Seconds:
                    return start.AddSeconds(Duration);
                case AggregationDurationType.Minutes:
                    return start.AddMinutes(Duration);
                case AggregationDurationType.Hours:
                    return start.AddHours(Duration);
                case AggregationDurationType.Days:
                    return start.AddDays(Duration);
                case AggregationDurationType.Months:
                    return start.AddMonths(Duration);
                case AggregationDurationType.Years:
                    return start.AddYears(Duration);
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public DateTimeOffset GetStartOfRangeForDateTime(DateTimeOffset pointAt)
        {
            pointAt = pointAt.ToUniversalTime();
            switch (Type)
            {
                case AggregationDurationType.Seconds:
                    return new DateTimeOffset(pointAt.Year, pointAt.Month, pointAt.Day, pointAt.Hour, pointAt.Minute, pointAt.Second / Duration * Duration, TimeSpan.Zero);
                case AggregationDurationType.Minutes:
                    return new DateTimeOffset(pointAt.Year, pointAt.Month, pointAt.Day, pointAt.Hour, pointAt.Minute / Duration * Duration, 0, TimeSpan.Zero);
                case AggregationDurationType.Hours:
                    return new DateTimeOffset(pointAt.Year, pointAt.Month, pointAt.Day, pointAt.Hour / Duration * Duration, 0, 0, TimeSpan.Zero);
                case AggregationDurationType.Days:
                    return new DateTimeOffset(pointAt.Year, pointAt.Month, pointAt.Day, 0, 0, 0, TimeSpan.Zero);
                case AggregationDurationType.Months:
                case AggregationDurationType.Years:
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}
