// -----------------------------------------------------------------------
//  <copyright file="AsyncMultiDatabaseHiLoIdGenerator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Identity
{
    public class AsyncMultiDatabaseHiLoIdGenerator : IHiLoIdGenerator
    {
        protected readonly DocumentStore Store;

        private readonly ConcurrentDictionary<string, AsyncMultiTypeHiLoIdGenerator> _generators =
            new ConcurrentDictionary<string, AsyncMultiTypeHiLoIdGenerator>();

        public AsyncMultiDatabaseHiLoIdGenerator(DocumentStore store)
        {
            Store = store;
        }

        public Task<string> GenerateDocumentIdAsync(string database, object entity)
        {
            database = Store.GetDatabase(database);
            var generator = _generators.GetOrAdd(database, GenerateAsyncMultiTypeHiLoFunc);
            return generator.GenerateDocumentIdAsync(entity);
        }

        public Task<long> GenerateNextIdForAsync(string database, object entity)
        {
            var collectionName = Store.Conventions.GetCollectionName(entity);
            return GenerateNextIdForAsync(database, collectionName);
        }

        public Task<long> GenerateNextIdForAsync(string database, Type type)
        {
            var collectionName = Store.Conventions.GetCollectionName(type);
            return GenerateNextIdForAsync(database, collectionName);
        }

        public Task<long> GenerateNextIdForAsync(string database, string collectionName)
        {
            database = Store.GetDatabase(database);
            var generator = _generators.GetOrAdd(database, GenerateAsyncMultiTypeHiLoFunc);
            return generator.GenerateNextIdForAsync(collectionName);
        }

        public virtual AsyncMultiTypeHiLoIdGenerator GenerateAsyncMultiTypeHiLoFunc(string database)
        {
            return new AsyncMultiTypeHiLoIdGenerator(Store, database);
        }

        public async Task ReturnUnusedRange()
        {
            foreach (var generator in _generators)
                await generator.Value.ReturnUnusedRange().ConfigureAwait(false);
        }
    }
}
