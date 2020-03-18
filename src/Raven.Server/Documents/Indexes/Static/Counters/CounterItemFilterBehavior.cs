using System.Collections.Generic;
using Sparrow.Platform;

namespace Raven.Server.Documents.Indexes.Static.Counters
{
    public class CounterItemFilterBehavior : IIndexItemFilterBehavior
    {
        private static int MaxCapacity = PlatformDetails.Is32Bits
            ? 1024
            : 1024 * 1024;

        private readonly HashSet<string> _seenIds = new HashSet<string>();

        public bool ShouldFilter(IndexItem item)
        {
            var key = item.LowerId;
            if (key == null)
                return true;

            var shouldFilter = _seenIds.Add(key) == false;

            if (shouldFilter == false && _seenIds.Count > MaxCapacity)
                _seenIds.Clear();

            return shouldFilter;
        }
    }
}
