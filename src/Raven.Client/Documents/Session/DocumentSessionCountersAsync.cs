//-----------------------------------------------------------------------
// <copyright file="DocumentSessionCountersAsync.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public class DocumentSessionCountersAsync : DocumentSessionCountersBase, ICountersSessionOperationsAsync
    {
        public DocumentSessionCountersAsync(InMemoryDocumentSessionOperations session) : base(session)
        {
        }

        public Task<Dictionary<string, long>> GetAsync(string documentId, CancellationToken token = default)
        {
            return DocumentStore.Counters.ForDatabase(Session.DatabaseName).GetAsync(documentId, new string[0], token);
        }

        public Task<Dictionary<string, long>> GetAsync(object entity, CancellationToken token = default)
        {
            if (DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSession(entity);

            return GetAsync(document.Id, token);
        }

        public Task<long?> GetAsync(string documentId, string counter, CancellationToken token = default)
        {
            return DocumentStore.Counters.ForDatabase(Session.DatabaseName).GetAsync(documentId, counter, token);
        }

        public Task<long?> GetAsync(object entity, string counter, CancellationToken token = default)
        {
            if (DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSession(entity);

            return GetAsync(document.Id, counter, token);
        }

        public Task<Dictionary<string, long>> GetAsync(string documentId, IEnumerable<string> counters, CancellationToken token = default)
        {
            return DocumentStore.Counters.ForDatabase(Session.DatabaseName).GetAsync(documentId, counters, token);
        }

        public Task<Dictionary<string, long>> GetAsync(object entity, IEnumerable<string> counters, CancellationToken token = default)
        {
            if (DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSession(entity);

            return GetAsync(document.Id, counters, token);
        }

        public Task<Dictionary<string, long>> GetAsync(string documentId, params string[] counters)
        {
            return DocumentStore.Counters.ForDatabase(Session.DatabaseName).GetAsync(documentId, counters);
        }

        public Task<Dictionary<string, long>> GetAsync(object entity, params string[] counters)
        {
            if (DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSession(entity);

            return GetAsync(document.Id, counters);
        }
    }
}
