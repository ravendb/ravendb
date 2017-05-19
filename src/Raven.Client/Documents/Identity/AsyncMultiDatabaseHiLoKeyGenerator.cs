// -----------------------------------------------------------------------
//  <copyright file="AsyncMultiDatabaseHiLoKeyGenerator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Concurrent;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;

namespace Raven.Client.Documents.Identity
{
    public class AsyncMultiDatabaseHiLoKeyGenerator
    {
        private readonly DocumentStore _store;
        private readonly DocumentConventions _conventions;

        private readonly ConcurrentDictionary<string, AsyncMultiTypeHiLoKeyGenerator> _generators =
            new ConcurrentDictionary<string, AsyncMultiTypeHiLoKeyGenerator>();

        public AsyncMultiDatabaseHiLoKeyGenerator(DocumentStore store, DocumentConventions conventions)
        {
            _store = store;
            _conventions = conventions;
        }


        public Task<string> GenerateDocumentKeyAsync(string dbName,
                                                     object entity)
        {
            var db = dbName ?? _store.Database;
            var generator = _generators.GetOrAdd(db, GenerateAsyncMultiTypeHiLoFunc);
            return generator.GenerateDocumentKeyAsync(entity);
        }

        public AsyncMultiTypeHiLoKeyGenerator GenerateAsyncMultiTypeHiLoFunc(string dbName)
        {
            return new AsyncMultiTypeHiLoKeyGenerator(_store, dbName, _conventions);
        }

        public async Task ReturnUnusedRange()
        {
            foreach (var generator in _generators)
            {
                await generator.Value.ReturnUnusedRange().ConfigureAwait(false);
            }
        }
    }
}
