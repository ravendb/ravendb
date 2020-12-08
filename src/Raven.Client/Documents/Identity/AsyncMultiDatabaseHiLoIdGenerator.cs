// -----------------------------------------------------------------------
//  <copyright file="AsyncMultiDatabaseHiLoIdGenerator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Concurrent;
using System.Threading.Tasks;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Identity
{
    public class AsyncMultiDatabaseHiLoIdGenerator
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
