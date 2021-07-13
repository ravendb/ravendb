//-----------------------------------------------------------------------
// <copyright file="AsyncMultiTypeHiLoIdGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
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
        private char _identityPartsSeparator;

        public AsyncMultiTypeHiLoIdGenerator(DocumentStore store, string dbName)
        {
            Store = store;
            DbName = dbName;
            Conventions = store.GetRequestExecutor(dbName).Conventions;
            _identityPartsSeparator = Conventions.IdentityPartsSeparator;
        }

        public async Task<string> GenerateDocumentIdAsync(object entity)
        {
            var identityPartsSeparator = Conventions.IdentityPartsSeparator;
            if (_identityPartsSeparator != identityPartsSeparator)
                await MaybeRefresh(identityPartsSeparator).ConfigureAwait(false);

            var typeTagName = Conventions.GetCollectionName(entity);
            if (string.IsNullOrEmpty(typeTagName)) //ignore empty tags
            {
                return null;
            }
            var tag = Conventions.TransformTypeCollectionNameToDocumentIdPrefix(typeTagName);
            if (_idGeneratorsByTag.TryGetValue(tag, out var value))
            {
                return await value.GenerateDocumentIdAsync().ConfigureAwait(false);
            }

            await _generatorLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_idGeneratorsByTag.TryGetValue(tag, out value))
                    return await value.GenerateDocumentIdAsync().ConfigureAwait(false);

                value = CreateGeneratorFor(tag);
                _idGeneratorsByTag.TryAdd(tag, value);
            }
            finally
            {
                _generatorLock.Release();
            }

            return await value.GenerateDocumentIdAsync().ConfigureAwait(false);
        }

        private async Task MaybeRefresh(char identityPartsSeparator)
        {
            List<AsyncHiLoIdGenerator> idGenerators = null;
            await _generatorLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_identityPartsSeparator == identityPartsSeparator)
                    return;

                idGenerators = _idGeneratorsByTag.Values.ToList();

                _idGeneratorsByTag.Clear();
                _identityPartsSeparator = identityPartsSeparator;
            }
            finally
            {
                _generatorLock.Release();
            }

            if (idGenerators != null)
            {
                try
                {
                    await ReturnUnusedRange(idGenerators).ConfigureAwait(false);
                }
                catch
                {
                    // ignored
                }
            }
        }

        protected virtual AsyncHiLoIdGenerator CreateGeneratorFor(string tag)
        {
            return new AsyncHiLoIdGenerator(tag, Store, DbName, _identityPartsSeparator);
        }

        public async Task ReturnUnusedRange()
        {
            await ReturnUnusedRange(_idGeneratorsByTag.Values).ConfigureAwait(false);
        }

        private static async Task ReturnUnusedRange(IEnumerable<AsyncHiLoIdGenerator> generators)
        {
            foreach (var generator in generators)
                await generator.ReturnUnusedRangeAsync().ConfigureAwait(false);
        }
    }
}
