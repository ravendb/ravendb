using System;

namespace RavenFS.Tests.Tools
{
    public static class TimeMeasure
    {
        public static TimeSpan HowLong(Action action)
        {
            var start = DateTime.Now;
            action();
            return DateTime.Now - start;
        }
    }
}
