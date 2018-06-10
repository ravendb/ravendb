//-----------------------------------------------------------------------
// <copyright file="SessionDocumentCounters.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;

namespace Raven.Client.Documents.Session
{
    public class SessionDocumentCounters : DocumentSessionCountersBase, ICountersSessionOperations
    {

        public SessionDocumentCounters(InMemoryDocumentSessionOperations session, string documentId) : base(session, documentId)
        {
        }
        public SessionDocumentCounters(InMemoryDocumentSessionOperations session, object entity) : base(session, entity)
        {
        }

        public Dictionary<string, long> GetAll()
        {
            return _session.DocumentStore.Counters.ForDatabase(_session.DatabaseName).GetAll(_docId);
        }

        public long? Get(string counter)
        {
            return _session.DocumentStore.Counters.ForDatabase(_session.DatabaseName).Get(_docId, counter);           
        }

        public Dictionary<string, long?> Get(IEnumerable<string> counters)
        {
            return _session.DocumentStore.Counters.ForDatabase(_session.DatabaseName).Get(_docId, counters);
        }

    }
}
