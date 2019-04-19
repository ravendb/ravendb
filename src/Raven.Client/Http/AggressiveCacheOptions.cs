using System;

namespace Raven.Client.Http
{
    public class AggressiveCacheOptions
    {
        public AggressiveCacheOptions(TimeSpan duration, AggressiveCacheMode mode)
        {
            Duration = duration;
            Mode = mode;
        }

        public TimeSpan Duration { get; set; }

        public AggressiveCacheMode Mode { get; set; }
    }
}
