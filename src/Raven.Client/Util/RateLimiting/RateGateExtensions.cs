using System;
using System.Collections.Generic;

namespace Raven.Client.Util.RateLimiting
{
    public static class RateGateExtensions
    {
        public static IEnumerable<T> LimitRate<T>(this IEnumerable<T> source, int count, TimeSpan timeUnit)
        {
            using (var rateGate = new RateGate(count, timeUnit))
            {
                foreach (var item in source)
                {
                    rateGate.WaitToProceed();
                    yield return item;
                }
            }
        }
    }
}