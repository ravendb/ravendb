//-----------------------------------------------------------------------
// <copyright file="SessionDocumentCounters.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using Raven.Client.Util;

namespace Raven.Client.Documents.Session
{
    public sealed class SessionDocumentCounters : ISessionDocumentCounters
    {
        private readonly AsyncSessionDocumentCounters _asyncSessionCounters;

        public SessionDocumentCounters(InMemoryDocumentSessionOperations session, string documentId)
        {
            _asyncSessionCounters = new AsyncSessionDocumentCounters(session, documentId);
        }
        public SessionDocumentCounters(InMemoryDocumentSessionOperations session, object entity)
        {
            _asyncSessionCounters = new AsyncSessionDocumentCounters(session, entity);
        }

        public Dictionary<string, long?> GetAll()
        {
            return AsyncHelpers.RunSync(() => _asyncSessionCounters.GetAllAsync());
        }

        public long? Get(string counter)
        {
            return AsyncHelpers.RunSync(() => _asyncSessionCounters.GetAsync(counter));
        }

        public Dictionary<string, long?> Get(IEnumerable<string> counters)
        {
            return AsyncHelpers.RunSync(() => _asyncSessionCounters.GetAsync(counters));
        }

        public void Increment(string counter, long delta = 1)
        {
            _asyncSessionCounters.Increment(counter, delta);
        }

        public void Delete(string counter)
        {
            _asyncSessionCounters.Delete(counter);
        }
    }
}
