//-----------------------------------------------------------------------
// <copyright file="DocumentSessionCountersAsync.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
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

        public Task<Dictionary<string, long>> GetAsync(string documentId)
        {
            return DocumentStore.Counters.GetAsync(documentId, new string[0]);
        }

        public Task<Dictionary<string, long>> GetAsync(object entity)
        {
            if (DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSession(entity);

            return GetAsync(document.Id);
        }

        public Task<long?> GetAsync(string documentId, string counter)
        {
            return DocumentStore.Counters.GetAsync(documentId, counter);
        }

        public Task<long?> GetAsync(object entity, string counter)
        {
            if (DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSession(entity);

            return GetAsync(document.Id, counter);
        }

        public Task<Dictionary<string, long>> GetAsync(string documentId, IEnumerable<string> counters)
        {
            return DocumentStore.Counters.GetAsync(documentId, counters);
        }

        public Task<Dictionary<string, long>> GetAsync(object entity, IEnumerable<string> counters)
        {
            if (DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSession(entity);

            return GetAsync(document.Id, counters);
        }

    }
}
