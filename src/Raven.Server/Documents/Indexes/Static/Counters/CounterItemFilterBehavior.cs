using System.Collections.Generic;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static.Counters
{
    public class CounterItemFilterBehavior : IIndexItemFilterBehavior
    {
        private readonly HashSet<LazyStringValue> _seenIds = new HashSet<LazyStringValue>();

        public bool ShouldFilter(IndexItem item)
        {
            var key = item.LowerId;
            if (key == null)
                return true;

            return _seenIds.Add(key) == false;
        }
    }
}
