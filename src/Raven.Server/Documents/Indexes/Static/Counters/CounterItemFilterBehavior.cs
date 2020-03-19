using System.Collections.Generic;
using Sparrow.Platform;

namespace Raven.Server.Documents.Indexes.Static.Counters
{
    /// <summary>
    /// This is used purely for optimization
    /// The way we store counters in the storage is not natural for indexing
    /// But those counter groups are the only entity that we can iterate over to increase the indexing etag
    /// Because of that we can encounter a duplicates when iterating over multiple counter groups and in this case duplicate is documentId|counterName pair
    /// To avoid re-indexing same thing in one batch, and yet still iterate over them (to increase etag), the filter was introduced
    /// </summary>
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
