//-----------------------------------------------------------------------
// <copyright file="AsyncMultiTypeHiLoIdGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;

namespace Raven.Client.Documents.Identity
{
    /// <summary>
    /// Generate a hilo ID for each given type
    /// </summary>
    public class AsyncMultiTypeHiLoIdGenerator
    {
        private readonly SemaphoreSlim _generatorLock = new SemaphoreSlim(1, 1);
        private readonly ConcurrentDictionary<string, AsyncHiLoIdGenerator> _idGeneratorsByTag = new ConcurrentDictionary<string, AsyncHiLoIdGenerator>();
        protected readonly DocumentStore Store;
        protected readonly string DbName;
        protected readonly DocumentConventions Conventions;

        public AsyncMultiTypeHiLoIdGenerator(DocumentStore store, string dbName, DocumentConventions conventions)
        {
            Store = store;
            DbName = dbName;
            Conventions = conventions;
        }

        public async Task<string> GenerateDocumentIdAsync(object entity)
        {
            var typeTagName = Conventions.GetCollectionName(entity);
            if (string.IsNullOrEmpty(typeTagName)) //ignore empty tags
            {
                return null;
            }
            var tag = Conventions.TransformTypeCollectionNameToDocumentIdPrefix(typeTagName);
            if (_idGeneratorsByTag.TryGetValue(tag, out var value))
            {
                return await value.GenerateDocumentIdAsync(entity).ConfigureAwait(false);
            }

            await _generatorLock.WaitAsync().ConfigureAwait(false);

            try
            {
                if (_idGeneratorsByTag.TryGetValue(tag, out value))
                    return await value.GenerateDocumentIdAsync(entity).ConfigureAwait(false);

                value = CreateGeneratorFor(tag);
                _idGeneratorsByTag.TryAdd(tag, value);

                return await value.GenerateDocumentIdAsync(entity).ConfigureAwait(false);
            }
            finally
            {
                _generatorLock.Release();
            }
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
