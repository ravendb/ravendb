using System;
using System.Net;
using Raven.Abstractions.Data;
using Raven.Server.Indexing.Corax.Queries;
using Raven.Server.Json;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Indexing.Corax
{
    public class Searcher : IDisposable
    {
        private readonly FullTextIndex _parent;
        private readonly RavenOperationContext _context;

        public Searcher(FullTextIndex parent)
        {
            _parent = parent;
            _context = new RavenOperationContext(_parent.Pool)
            {
                Transaction = _parent.Env.ReadTransaction()
            };
            _context.CachedProperties.Version = 1;
        }

        public unsafe string[] Query(Query query, int take)
        {
            var entries = new Table(_parent.EntriesSchema, "IndexEntries", _context.Transaction);
            query.Initialize(_parent, _context, entries);
            var queryMatches = query.Execute();

            Array.Sort(queryMatches, QueryMatchScoreSorter.Instance);

            var entryId = 0L;
            var entryKey = new Slice((byte*)&entryId, sizeof(long));
            var results = new string[Math.Min(queryMatches.Length, take)];
            for (int i = 0; i < results.Length; i++)
            {
                entryId = IPAddress.NetworkToHostOrder(queryMatches[i].DocumentId);
                var tvr = entries.ReadByKey(entryKey);
                int size;
                var entry = new BlittableJsonReaderObject(tvr.Read(1, out size), size, _context);
                entry.TryGet(Constants.DocumentIdFieldName, out results[i]);
            }

            return results;
        }

        public void Dispose()
        {
            _context?.Dispose();
        }
    }
}