//-----------------------------------------------------------------------
// <copyright file="AsyncSessionDocumentCounters.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Session
{
    public class AsyncSessionDocumentCounters : DocumentSessionCountersBase, ICountersSessionOperationsAsync
    {
        public AsyncSessionDocumentCounters(InMemoryDocumentSessionOperations session, string documentId) : base(session, documentId)
        {
        }

        public AsyncSessionDocumentCounters(InMemoryDocumentSessionOperations session, object entity) : base(session, entity)
        {
        }

        public Task<Dictionary<string, long>> GetAllAsync(CancellationToken token = default(CancellationToken))
        {
            return _session.DocumentStore.Counters.ForDatabase(_session.DatabaseName).GetAllAsync(_docId);
        }

        public Task<long?> GetAsync(string counter, CancellationToken token = default(CancellationToken))
        {
            return _session.DocumentStore.Counters.ForDatabase(_session.DatabaseName).GetAsync(_docId, counter);
        }

        public Task<Dictionary<string, long?>> GetAsync(IEnumerable<string> counters, CancellationToken token = default(CancellationToken))
        {
            return _session.DocumentStore.Counters.ForDatabase(_session.DatabaseName).GetAsync(_docId, counters, token);
        }

    }
}
