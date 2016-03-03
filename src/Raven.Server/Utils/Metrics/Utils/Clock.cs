
using System;
using System.Diagnostics;

namespace Metrics.Utils
{
    public class Clock
    {
        public const int NANOSECONDS_IN_SECOND = 1000 * 1000 * 1000;
        public const int NANOSECONDS_IN_MILISECOND = 1000 * 1000;

        public static readonly long FrequencyFactor = (1000L * 1000L * 1000L) / Stopwatch.Frequency;
        public static long Nanoseconds { get { return Stopwatch.GetTimestamp() * FrequencyFactor; } }
        public static DateTime UTCDateTime { get { return DateTime.UtcNow; } }
        public static long Seconds { get { return Nanoseconds/NANOSECONDS_IN_SECOND; } }
    }
}
