// -----------------------------------------------------------------------
//  <copyright file="MultiDatabaseHiLoGenerator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Concurrent;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Connection;

namespace Raven.NewClient.Client.Document
{
    public class MultiDatabaseHiLoGenerator
    {

        private readonly ConcurrentDictionary<string, MultiTypeHiLoKeyGenerator> _generators =
            new ConcurrentDictionary<string, MultiTypeHiLoKeyGenerator>();

        private readonly DocumentStore _store;
        private readonly DocumentConvention _conventions;

        public MultiDatabaseHiLoGenerator(DocumentStore store, DocumentConvention conventions)
        {
            _store = store;
            _conventions = conventions;
        }

        public string GenerateDocumentKey(string dbName, object entity)
        {
            var db = dbName ?? _store.DefaultDatabase;
            var multiTypeHiLoKeyGenerator = _generators.GetOrAdd(db, s => new MultiTypeHiLoKeyGenerator(_store, db, _conventions));
            return multiTypeHiLoKeyGenerator.GenerateDocumentKey(entity);
        }

        public void ReturnUnusedRange()
        {
            foreach (var generator in _generators)
            {
                generator.Value.ReturnUnusedRange();
            }
        }
    }
}
