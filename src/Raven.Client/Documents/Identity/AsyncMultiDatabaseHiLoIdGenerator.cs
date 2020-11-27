// -----------------------------------------------------------------------
//  <copyright file="AsyncMultiDatabaseHiLoIdGenerator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Concurrent;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Identity
{
    public class AsyncMultiDatabaseHiLoIdGenerator
    {
        protected readonly DocumentStore Store;
        protected readonly DocumentConventions Conventions;

        private readonly ConcurrentDictionary<string, AsyncMultiTypeHiLoIdGenerator> _generators =
            new ConcurrentDictionary<string, AsyncMultiTypeHiLoIdGenerator>();

        public AsyncMultiDatabaseHiLoIdGenerator(DocumentStore store, DocumentConventions conventions)
        {
            Store = store;
            Conventions = conventions;
        }


        public Task<string> GenerateDocumentIdAsync(string dbName, object entity)
        {
            var database = Store.GetDatabase(dbName);
            var generator = _generators.GetOrAdd(database, GenerateAsyncMultiTypeHiLoFunc);
            return generator.GenerateDocumentIdAsync(entity);
        }

        public virtual AsyncMultiTypeHiLoIdGenerator GenerateAsyncMultiTypeHiLoFunc(string dbName)
        {
            return new AsyncMultiTypeHiLoIdGenerator(Store, dbName, Conventions);
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
