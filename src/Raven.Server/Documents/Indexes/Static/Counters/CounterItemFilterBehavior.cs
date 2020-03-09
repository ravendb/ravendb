using System.Collections.Generic;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static.Counters
{
    public class CounterItemFilterBehavior : IIndexItemFilterBehavior
    {
        private readonly HashSet<LazyStringValue> _seenIds = new HashSet<LazyStringValue>();

        public bool ShouldFilter(IndexItem item)
        {
            return _seenIds.Add(item.LowerId) == false;
        }
    }
}
