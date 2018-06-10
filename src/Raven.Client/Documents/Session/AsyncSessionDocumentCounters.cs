//-----------------------------------------------------------------------
// <copyright file="AsyncSessionDocumentCounters.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
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
                cache = new InMemoryDocumentSessionOperations.CountersCache
                {
                    Values = new Dictionary<string, long?>()
                };
            }

            cache.GotAll = true;
            Session.CountersByDocId[DocId] = cache;
            Session.IncrementRequestCount();

            var vals = await Session.DocumentStore.Counters.ForDatabase(Session.DatabaseName).GetAllAsync(DocId).ConfigureAwait(false);
            foreach (var kvp in vals)
            {
                cache.Values[kvp.Key] = kvp.Value;
            }

            return vals;
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
                cache = new InMemoryDocumentSessionOperations.CountersCache
                {
                    Values = new Dictionary<string, long?>()
                };
                Session.CountersByDocId.Add(DocId, cache);
            }

            Session.IncrementRequestCount();
            value = await Session.DocumentStore.Counters.ForDatabase(Session.DatabaseName).GetAsync(DocId, counter).ConfigureAwait(false);
            cache.Values.Add(counter, value);

            return value;
        }

        public async Task<Dictionary<string, long?>> GetAsync(IEnumerable<string> counters, CancellationToken token = default(CancellationToken))
        {
            var countersToGetFromServer = new List<string>();
            var result = new Dictionary<string, long?>();

            if (Session.CountersByDocId.TryGetValue(DocId, out var cache))
            {
                if (cache.GotAll)
                {
                    foreach (var counter in counters)
                    {
                        cache.Values.TryGetValue(counter, out var val);
                        result[counter] = val;
                    }

                    return result;
                }

                foreach (var counter in counters)
                {
                    if (cache.Values.TryGetValue(counter, out var val))
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
                cache = new InMemoryDocumentSessionOperations.CountersCache
                {
                    Values = new Dictionary<string, long?>()
                };
                Session.CountersByDocId.Add(DocId, cache);
                countersToGetFromServer = counters.ToList();
            }

            if (countersToGetFromServer.Count > 0)
            {
                Session.IncrementRequestCount();

                var vals = await Session.DocumentStore.Counters.ForDatabase(Session.DatabaseName).GetAsync(DocId, countersToGetFromServer).ConfigureAwait(false);
                foreach (var kvp in vals)
                {
                    cache.Values[kvp.Key] = kvp.Value;
                    result[kvp.Key] = kvp.Value;
                }
            }

            return result;
        }
    }
}
