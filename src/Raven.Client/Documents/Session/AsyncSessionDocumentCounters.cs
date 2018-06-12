//-----------------------------------------------------------------------
// <copyright file="AsyncSessionDocumentCounters.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Session
{
    public class AsyncSessionDocumentCounters : SessionCountersBase, ICountersSessionOperationsAsync
    {
        public AsyncSessionDocumentCounters(InMemoryDocumentSessionOperations session, string documentId) : base(session, documentId)
        {
        }

        public AsyncSessionDocumentCounters(InMemoryDocumentSessionOperations session, object entity) : base(session, entity)
        {
        }

        public async Task<long?> GetAsync(string counter, CancellationToken token = default(CancellationToken))
        {
            long? value;

            if (Session.CountersByDocId.TryGetValue(DocId, out var cache))
            {
                if (cache.Values.TryGetValue(counter, out value) || cache.GotAll)
                {
                    return value;
                }

            }
            else
            {
                cache = new InMemoryDocumentSessionOperations.CountersCache();
            }

            Session.IncrementRequestCount();
            value = await Session.DocumentStore.Counters.ForDatabase(Session.DatabaseName).GetAsync(DocId, counter).ConfigureAwait(false);
            cache.AddOrUptadeCounterValue(counter, value);

            Session.CountersByDocId[DocId] = cache;
            return value;
        }

        public async Task<Dictionary<string, long?>> GetAsync(IEnumerable<string> counters, CancellationToken token = default(CancellationToken))
        {
            var countersToGetFromServer = new List<string>();
            var result = new Dictionary<string, long?>();

            if (Session.CountersByDocId.TryGetValue(DocId, out var cache))
            {
                foreach (var counter in counters)
                {
                    if (cache.Values.TryGetValue(counter, out var val) || cache.GotAll)
                    {
                        result[counter] = val;
                    }
                    else
                    {
                        countersToGetFromServer.Add(counter);
                    }
                }

            }
            else
            {
                cache = new InMemoryDocumentSessionOperations.CountersCache();
                countersToGetFromServer = counters.ToList();
            }

            if (countersToGetFromServer.Count > 0)
            {
                Session.IncrementRequestCount();

                var vals = await Session.DocumentStore.Counters.ForDatabase(Session.DatabaseName).GetAsync(DocId, countersToGetFromServer).ConfigureAwait(false);
                foreach (var kvp in vals)
                {
                    cache.AddOrUptadeCounterValue(kvp.Key, kvp.Value);
                    result[kvp.Key] = kvp.Value;
                }

                Session.CountersByDocId[DocId] = cache;

            }

            return result;
        }

        public async Task<Dictionary<string, long>> GetAllAsync(CancellationToken token = default(CancellationToken))
        {
            if (Session.CountersByDocId.TryGetValue(DocId, out var cache))
            {
                if (cache.GotAll)
                {
                    var result = new Dictionary<string, long>();
                    foreach (var kvp in cache.Values)
                    {
                        if (kvp.Value.HasValue)
                        {
                            result.Add(kvp.Key, kvp.Value.Value);
                        }
                    }

                    return result;
                }

            }
            else
            {
                cache = new InMemoryDocumentSessionOperations.CountersCache();
            }

            Session.IncrementRequestCount();

            Dictionary<string, long> values;

            if (cache.MissingCounters != null)
            {
                values = cache.Values
                    .Where(kvp => kvp.Value.HasValue)
                    .ToDictionary(kvp => kvp.Key, kvp => kvp.Value.Value);

                var missingValues = await Session.DocumentStore.Counters.ForDatabase(Session.DatabaseName)
                    .GetAsync(DocId, cache.MissingCounters).ConfigureAwait(false);

                foreach (var kvp in missingValues)
                {
                    if (kvp.Value.HasValue == false)
                        continue;

                    values[kvp.Key] = kvp.Value.Value;
                    cache.Values[kvp.Key] = kvp.Value;
                }

            }
            else
            {
                values = await Session.DocumentStore.Counters.ForDatabase(Session.DatabaseName).GetAllAsync(DocId).ConfigureAwait(false);
                foreach (var kvp in values)
                {
                    cache.Values[kvp.Key] = kvp.Value;
                }
            }

            cache.GotAll = true;
            Session.CountersByDocId[DocId] = cache;
            return values;
        }
    }
}
