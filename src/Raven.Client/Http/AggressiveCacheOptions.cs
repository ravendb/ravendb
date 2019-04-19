using System;

namespace Raven.Client.Http
{
    public class AggressiveCacheOptions
    {
        public AggressiveCacheOptions(TimeSpan duration, AggressiveCacheMode mode)
        {
            if (mode == AggressiveCacheMode.Unknown)
                throw new ArgumentException($"Mode must not be set to '{AggressiveCacheMode.Unknown}' value", nameof(mode));

            Duration = duration;
            Mode = mode;
        }

        public TimeSpan Duration { get; private set; }

        public AggressiveCacheMode Mode { get; private set; }
    }
}
