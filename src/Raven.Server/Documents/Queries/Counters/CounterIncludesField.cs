using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.Queries.Counters
{
    public sealed class CounterIncludesField
    {
        public CounterIncludesField()
        {
            Counters = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
        }

        public readonly Dictionary<string, HashSet<string>> Counters;

        public void AddCounter(string counter, string sourcePath = null)
        {
            var key = sourcePath ?? string.Empty;
            if (Counters.TryGetValue(key, out var hashSet) == false)
            {
                hashSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                Counters.Add(key, hashSet);
            }
            hashSet.Add(counter);
        }
    }
}
