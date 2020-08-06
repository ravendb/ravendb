//-----------------------------------------------------------------------
// <copyright file="AsyncSessionDocumentCounters.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Counters;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    public class AsyncSessionDocumentCounters : SessionCountersBase, IAsyncSessionDocumentCounters
    {
        public AsyncSessionDocumentCounters(InMemoryDocumentSessionOperations session, string documentId) : base(session, documentId)
        {
        }

        public AsyncSessionDocumentCounters(InMemoryDocumentSessionOperations session, object entity) : base(session, entity)
        {
        }

        public async Task<long?> GetAsync(string counter, CancellationToken token = default)
        {
            using (Session.AsyncTaskHolder())
            {
                long? value = null;
                if (Session.CountersByDocId.TryGetValue(DocId, out var cache))
                {
                    if (cache.Values.TryGetValue(counter, out value))
                        return value;
                }
                else
                {
                    cache.Values = new Dictionary<string, long?>(StringComparer.OrdinalIgnoreCase);
                }

                if ((Session.DocumentsById.TryGetValue(DocId, out var document) == false && cache.GotAll == false) ||
                    (document != null && document.Metadata.TryGet(Constants.Documents.Metadata.Counters,
                         out BlittableJsonReaderArray metadataCounters) &&
                     metadataCounters.BinarySearch(counter, StringComparison.OrdinalIgnoreCase) >= 0))

                {
                    // we either don't have the document in session and GotAll = false,
                    // or we do and it's metadata contains the counter name

                    Session.IncrementRequestCount();

                    var details = await Session.Operations.SendAsync(
                            new GetCountersOperation(DocId, counter), Session._sessionInfo, token: token)
                        .ConfigureAwait(false);

                    if (details.Counters?.Count > 0)
                    {
                        value = details.Counters[0]?.TotalValue;
                    }

                }

                cache.Values[counter] = value;

                if (Session.NoTracking == false)
                    Session.CountersByDocId[DocId] = cache;

                return value;
            }
        }

        public async Task<Dictionary<string, long?>> GetAsync(IEnumerable<string> counters, CancellationToken token = default)
        {
            using (Session.AsyncTaskHolder())
            {
                if (Session.CountersByDocId.TryGetValue(DocId, out var cache) == false)
                {
                    cache.Values = new Dictionary<string, long?>(StringComparer.OrdinalIgnoreCase);
                }

                BlittableJsonReaderArray metadataCounters = null;

                Session.DocumentsById.TryGetValue(DocId, out var document);
                document?.Metadata.TryGet(Constants.Documents.Metadata.Counters, out metadataCounters);

                var result = new Dictionary<string, long?>();
                var countersArray = counters.ToArray();

                foreach (var counter in countersArray)
                {
                    if (cache.Values.TryGetValue(counter, out var val) ||
                        (document != null &&
                         metadataCounters?.BinarySearch(counter, StringComparison.OrdinalIgnoreCase) < 0) ||
                        cache.GotAll)
                    {
                        // we either have value in cache,
                        // or we have the metadata and the counter is not there,
                        // or GotAll

                        result[counter] = val;
                        continue;
                    }

                    result.Clear();
                    Session.IncrementRequestCount();

                    var details = await Session.Operations.SendAsync(new GetCountersOperation(DocId, countersArray), Session._sessionInfo, token: token)
                        .ConfigureAwait(false);

                    foreach (var counterDetail in details.Counters)
                    {
                        if (counterDetail == null)
                            continue;
                        cache.Values[counterDetail.CounterName] = counterDetail.TotalValue;
                        result[counterDetail.CounterName] = counterDetail.TotalValue;
                    }

                    break;
                }

                if (Session.NoTracking == false)
                    Session.CountersByDocId[DocId] = cache;

                return result;
            }
        }

        public async Task<Dictionary<string, long?>> GetAllAsync(CancellationToken token = default)
        {
            using (Session.AsyncTaskHolder())
            {
                if (Session.CountersByDocId.TryGetValue(DocId, out var cache) == false)
                {
                    cache.Values = new Dictionary<string, long?>(StringComparer.OrdinalIgnoreCase);
                }

                var missingCounters = cache.GotAll == false;

                if (Session.DocumentsById.TryGetValue(DocId, out var document))
                {
                    if (document.Metadata.TryGet(Constants.Documents.Metadata.Counters,
                            out BlittableJsonReaderArray metadataCounters) == false)
                    {
                        missingCounters = false;
                    }

                    else if (cache.Values.Count >= metadataCounters.Length)
                    {
                        missingCounters = false;
                        foreach (var c in metadataCounters)
                        {
                            if (cache.Values.ContainsKey(c.ToString()))
                                continue;
                            missingCounters = true;
                            break;
                        }
                    }

                }

                if (missingCounters)
                {
                    // we either don't have the document in session and GotAll = false,
                    // or we do and cache doesn't contain all metadata counters

                    Session.IncrementRequestCount();

                    var details = await Session.Operations.SendAsync(new GetCountersOperation(DocId), Session._sessionInfo, token: token)
                        .ConfigureAwait(false);

                    cache.Values.Clear();

                    foreach (var counterDetail in details.Counters)
                    {
                        cache.Values[counterDetail.CounterName] = counterDetail.TotalValue;
                    }
                }

                cache.GotAll = true;

                if (Session.NoTracking == false)
                    Session.CountersByDocId[DocId] = cache;

                return cache.Values;
            }
        }
    }
}
