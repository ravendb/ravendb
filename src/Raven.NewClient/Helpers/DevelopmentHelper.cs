using System;
using Raven.NewClient.Abstractions;

namespace Raven.NewClient.Client.Helpers
{
    internal static class DevelopmentHelper
    {
        public static void TimeBomb()
        {
            if (SystemTime.UtcNow > new DateTime(2017, 3, 1))
                throw new NotImplementedException("Development time bomb.");
        }
    }
}