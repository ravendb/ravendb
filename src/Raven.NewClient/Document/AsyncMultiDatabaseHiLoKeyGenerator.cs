// -----------------------------------------------------------------------
//  <copyright file="AsyncMultiDatabaseHiLoKeyGenerator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Data;


namespace Raven.NewClient.Client.Document
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
            var generator = _generators.GetOrAdd(db, s => new AsyncMultiTypeHiLoKeyGenerator(_store, db, _conventions));
            return generator.GenerateDocumentKeyAsync(entity);
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
