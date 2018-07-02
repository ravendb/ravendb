using System;
using System.Collections.Generic;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.Counters
{
    public class CounterIncludesField
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

        public void AddCounters(BlittableJsonReaderArray counters, string sourcePath = null)
        {
            foreach (var counter in counters)
            {
                AddCounter(counter.ToString(), sourcePath);
            }
        }

    }
}
