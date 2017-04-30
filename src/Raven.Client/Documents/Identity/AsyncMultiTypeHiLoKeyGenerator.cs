//-----------------------------------------------------------------------
// <copyright file="MultiTypeHiLoKeyGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Concurrent;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Util;

namespace Raven.Client.Documents.Identity
{
    /// <summary>
    /// Generate a hilo key for each given type
    /// </summary>
    public class AsyncMultiTypeHiLoKeyGenerator
    {
        //private readonly int capacity;
        private readonly object _generatorLock = new object();
        private readonly ConcurrentDictionary<string, AsyncHiLoKeyGenerator> _keyGeneratorsByTag = new ConcurrentDictionary<string, AsyncHiLoKeyGenerator>();
        private readonly DocumentStore _store;
        private readonly string _dbName;
        private readonly DocumentConventions _conventions;
        private static readonly Task<string> NullStringCompletedTask = Task.FromResult<string>(null);

        public AsyncMultiTypeHiLoKeyGenerator(DocumentStore store, string dbName, DocumentConventions conventions)
        {
            _store = store;
            _dbName = dbName;
            _conventions = conventions;
        }

        public Task<string> GenerateDocumentKeyAsync(object entity)
        {
            var typeTagName = _conventions.GetCollectionName(entity);
            if (string.IsNullOrEmpty(typeTagName)) //ignore empty tags
            {
                return NullStringCompletedTask;
            }
            var tag = _conventions.TransformTypeCollectionNameToDocumentIdPrefix(typeTagName);
            AsyncHiLoKeyGenerator value;
            if (_keyGeneratorsByTag.TryGetValue(tag, out value))
                return value.GenerateDocumentKeyAsync(entity);

            lock (_generatorLock)
            {
                if (_keyGeneratorsByTag.TryGetValue(tag, out value))
                    return value.GenerateDocumentKeyAsync(entity);

                value = new AsyncHiLoKeyGenerator(tag, _store, _dbName, _conventions.IdentityPartsSeparator);
                _keyGeneratorsByTag.TryAdd(tag, value);
            }

            return value.GenerateDocumentKeyAsync(entity);
        }

        public async Task ReturnUnusedRange()
        {
            foreach (var generator in _keyGeneratorsByTag)
            {
                await generator.Value.ReturnUnusedRangeAsync().ConfigureAwait(false);
            }
        }
    }
}
