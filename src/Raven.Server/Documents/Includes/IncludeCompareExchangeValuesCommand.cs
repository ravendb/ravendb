using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using System.Diagnostics;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Util;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Includes
{
    public sealed class IncludeCompareExchangeValuesCommand : ICompareExchangeValueIncludes, IDisposable
    {
        private readonly ServerStore _serverStore;
        private readonly AbstractCompareExchangeStorage _compareExchangeStorage;
        private readonly char _identityPartsSeparator;

        private readonly string[] _includes;

        private HashSet<string> _includedKeys;

        private IDisposable _releaseContext;
        private ClusterOperationContext _serverContext;
        private readonly bool _throwWhenServerContextIsAllocated;
        public Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>> Results { get; set; }

        private IncludeCompareExchangeValuesCommand([NotNull] DocumentDatabase database, ClusterOperationContext serverContext, bool throwWhenServerContextIsAllocated, string[] compareExchangeValues)
            : this(database.ServerStore, database.CompareExchangeStorage, database.IdentityPartsSeparator, serverContext, throwWhenServerContextIsAllocated, compareExchangeValues)
        {
        }

        private IncludeCompareExchangeValuesCommand(ServerStore serverStore, AbstractCompareExchangeStorage compareExchangeStorage, char identityPartsSeparator, ClusterOperationContext serverContext, bool throwWhenServerContextIsAllocated, string[] compareExchangeValues)
        {
            _identityPartsSeparator = identityPartsSeparator;
            _serverStore = serverStore;
            _compareExchangeStorage = compareExchangeStorage;

            _serverContext = serverContext;
            _throwWhenServerContextIsAllocated = throwWhenServerContextIsAllocated;
            _includes = compareExchangeValues;
        }

        public static IncludeCompareExchangeValuesCommand ExternalScope(QueryOperationContext context, string[] compareExchangeValues)
        {
            return new IncludeCompareExchangeValuesCommand(context.Documents.DocumentDatabase, context.Server, throwWhenServerContextIsAllocated: true, compareExchangeValues);
        }

        public static IncludeCompareExchangeValuesCommand InternalScope(DocumentDatabase database, string[] compareExchangeValues)
        {
            return new IncludeCompareExchangeValuesCommand(database, serverContext: null, throwWhenServerContextIsAllocated: false, compareExchangeValues);
        }

        internal void AddRange(HashSet<string> keys)
        {
            if (keys == null)
                return;

            if (_includedKeys == null)
            {
                _includedKeys = new HashSet<string>(keys, StringComparer.OrdinalIgnoreCase);
                return;
            }

            foreach (var key in keys)
                _includedKeys.Add(key);
        }

        internal void Gather(Document document)
        {
            if (document == null)
                return;

            if (_includes == null || _includes.Length == 0)
                return;

            _includedKeys ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var include in _includes)
                IncludeUtil.GetDocIdFromInclude(document.Data, new StringSegment(include), _includedKeys, _identityPartsSeparator);
        }

        public void AddDocument(string id)
        {
            _includedKeys ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _includedKeys.Add(id);
        }

        public bool TryGetCompareExchange(string key, long? maxAtomicGuardIndex, out long index, out BlittableJsonReaderObject value)
        {
            index = -1;
            value = null;

            if (_serverContext == null)
                CreateServerContext();

            var result = _compareExchangeStorage.GetCompareExchangeValue(_serverContext, key);

            Debug.Assert(ClusterWideTransactionHelper.IsAtomicGuardKey(key) == false || maxAtomicGuardIndex.HasValue);
            if (ClusterWideTransactionHelper.IsAtomicGuardKey(key) && maxAtomicGuardIndex.HasValue && result.Index > maxAtomicGuardIndex)
                return false; // we are seeing partially committed value, skip it

            if (result.Index < 0)
                return false;
            
            index = result.Index;
            value = result.Value;
            return true;
        }

        internal void Materialize(long? maxAllowedAtomicGuardIndex)
        {
            if (_includedKeys == null || _includedKeys.Count == 0)
                return;

            foreach (var includedKey in _includedKeys)
            {
                if (string.IsNullOrEmpty(includedKey))
                    continue;

                var toAdd = TryGetCompareExchange(includedKey, maxAllowedAtomicGuardIndex, out var index, out var value)
                    ? (index, value)
                    : (-1, null);
                
                Results ??= new Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>>(StringComparer.OrdinalIgnoreCase);
                Results.Add(includedKey, new CompareExchangeValue<BlittableJsonReaderObject>(includedKey, toAdd.index,  toAdd.value));
            }
        }

        private void CreateServerContext()
        {
            if (_throwWhenServerContextIsAllocated)
                throw new InvalidOperationException("Cannot allocate new server context during materialization of compare exchange includes.");

            _releaseContext = _serverStore.Engine.ContextPool.AllocateOperationContext(out _serverContext);
            _serverContext.OpenReadTransaction();
        }

        public void Dispose()
        {
            _releaseContext?.Dispose();
            _releaseContext = null;
        }
    }
}
