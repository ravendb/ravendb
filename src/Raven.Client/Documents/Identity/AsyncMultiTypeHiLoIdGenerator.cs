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
        private readonly DocumentStore _store;
        private readonly string _dbName;
        private readonly DocumentConventions _conventions;
        private static readonly Task<string> NullStringCompletedTask = Task.FromResult<string>(null);

        public AsyncMultiTypeHiLoIdGenerator(DocumentStore store, string dbName, DocumentConventions conventions)
        {
            _store = store;
            _dbName = dbName;
            _conventions = conventions;
        }

        public Task<string> GenerateDocumentIdAsync(object entity)
        {
            var typeTagName = _conventions.GetCollectionName(entity);
            if (string.IsNullOrEmpty(typeTagName)) //ignore empty tags
            {
                return NullStringCompletedTask;
            }
            var tag = _conventions.TransformTypeCollectionNameToDocumentIdPrefix(typeTagName);
            AsyncHiLoIdGenerator value;
            if (_idGeneratorsByTag.TryGetValue(tag, out value))
                return value.GenerateDocumentIdAsync(entity);

            lock (_generatorLock)
            {
                if (_idGeneratorsByTag.TryGetValue(tag, out value))
                    return value.GenerateDocumentIdAsync(entity);

                value = new AsyncHiLoIdGenerator(tag, _store, _dbName, _conventions.IdentityPartsSeparator);
                _idGeneratorsByTag.TryAdd(tag, value);
            }

            return value.GenerateDocumentIdAsync(entity);
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
