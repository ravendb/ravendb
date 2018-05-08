//-----------------------------------------------------------------------
// <copyright file="DocumentSessionCounters.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public class DocumentSessionCounters : DocumentSessionCountersBase, ICountersSessionOperations
    {
        public DocumentSessionCounters(InMemoryDocumentSessionOperations session) : base(session)
        {
        }

        public Dictionary<string, long> Get(string documentId)
        {
            return DocumentStore.Counters.ForDatabase(Session.DatabaseName).Get(documentId, new string[0]);
        }

        public Dictionary<string, long> Get(object entity)
        {
            if (DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSession(entity);

            return Get(document.Id);
        }

        public long? Get(string documentId, string counter)
        {
            return DocumentStore.Counters.ForDatabase(Session.DatabaseName).Get(documentId, counter);
        }

        public long? Get(object entity, string counter)
        {
            if (DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSession(entity);

            return Get(document.Id, counter);
        }

        public Dictionary<string, long> Get(string documentId, IEnumerable<string> counters)
        {
            return DocumentStore.Counters.ForDatabase(Session.DatabaseName).Get(documentId, counters);
        }

        Dictionary<string, long> ICountersSessionOperations.Get(object entity, IEnumerable<string> counters)
        {
            if (DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSession(entity);

            return Get(document.Id, counters);
        }

        public Dictionary<string, long> Get(string documentId, params string[] counters)
        {
            return DocumentStore.Counters.ForDatabase(Session.DatabaseName).Get(documentId, counters);
        }

        public Dictionary<string, long> Get(object entity, params string[] counters)
        {
            if (DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSession(entity);

            return Get(document.Id, counters);
        }
    }
}
