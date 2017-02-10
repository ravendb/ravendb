// -----------------------------------------------------------------------
//  <copyright file="AsyncMultiDatabaseHiLoKeyGenerator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace Raven.Client.Document.Async
{
    public class AsyncMultiDatabaseHiLoKeyGenerator
    {
        private readonly DocumentStore _store;
        private readonly DocumentConvention _conventions;

        private readonly ConcurrentDictionary<string, AsyncMultiTypeHiLoKeyGenerator> _generators =
            new ConcurrentDictionary<string, AsyncMultiTypeHiLoKeyGenerator>();

        public AsyncMultiDatabaseHiLoKeyGenerator(DocumentStore store, DocumentConvention conventions)
        {
            _store = store;
            _conventions = conventions;
        }


        public Task<string> GenerateDocumentKeyAsync(string dbName,
                                                     object entity)
        {
            var db = dbName ?? _store.DefaultDatabase;
            var generator = _generators.GetOrAdd(db, GenrateAsyncMultiTypeHiLoFunc);
            return generator.GenerateDocumentKeyAsync(entity);
        }

        public  AsyncMultiTypeHiLoKeyGenerator GenrateAsyncMultiTypeHiLoFunc(string dbName)
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
