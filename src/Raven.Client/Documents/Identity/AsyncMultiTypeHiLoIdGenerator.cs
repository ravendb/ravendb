//-----------------------------------------------------------------------
// <copyright file="AsyncMultiTypeHiLoIdGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Concurrent;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;

namespace Raven.Client.Documents.Identity
{
    /// <summary>
    /// Generate a hilo ID for each given type
    /// </summary>
    public class AsyncMultiTypeHiLoIdGenerator
    {
        //private readonly int capacity;
        private readonly object _generatorLock = new object();
        private readonly ConcurrentDictionary<string, AsyncHiLoIdGenerator> _idGeneratorsByTag = new ConcurrentDictionary<string, AsyncHiLoIdGenerator>();
        protected readonly DocumentStore Store;
        protected readonly string DbName;
        protected readonly DocumentConventions Conventions;
        private static readonly Task<string> NullStringCompletedTask = Task.FromResult<string>(null);

        public AsyncMultiTypeHiLoIdGenerator(DocumentStore store, string dbName, DocumentConventions conventions)
        {
            Store = store;
            DbName = dbName;
            Conventions = conventions;
        }

        public Task<string> GenerateDocumentIdAsync(object entity)
        {
            var typeTagName = Conventions.GetCollectionName(entity);
            if (string.IsNullOrEmpty(typeTagName)) //ignore empty tags
            {
                return NullStringCompletedTask;
            }
            var tag = Conventions.TransformTypeCollectionNameToDocumentIdPrefix(typeTagName);
            if (_idGeneratorsByTag.TryGetValue(tag, out var value))
                return value.GenerateDocumentIdAsync(entity);

            lock (_generatorLock)
            {
                if (_idGeneratorsByTag.TryGetValue(tag, out value))
                    return value.GenerateDocumentIdAsync(entity);

                value = CreateGeneratorFor(tag);
                _idGeneratorsByTag.TryAdd(tag, value);
            }

            return value.GenerateDocumentIdAsync(entity);
        }

        protected virtual AsyncHiLoIdGenerator CreateGeneratorFor(string tag)
        {
            return new AsyncHiLoIdGenerator(tag, Store, DbName, Conventions.IdentityPartsSeparator);
        }

        public async Task ReturnUnusedRange()
        {
            foreach (var generator in _idGeneratorsByTag)
            {
                await generator.Value.ReturnUnusedRangeAsync().ConfigureAwait(false);
            }
        }
    }
}
