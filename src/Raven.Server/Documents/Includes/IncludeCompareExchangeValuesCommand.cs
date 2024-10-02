using System;
using System.Collections.Generic;
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
    public class IncludeCompareExchangeValuesCommand : IDisposable
    {
        private readonly DocumentDatabase _database;
        private readonly string[] _includes;

        private HashSet<string> _includedKeys;

        private IDisposable _releaseContext;
        private TransactionOperationContext _serverContext;
        private readonly bool _throwWhenServerContextIsAllocated;
        public Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>> Results;

        private IncludeCompareExchangeValuesCommand(DocumentDatabase database, TransactionOperationContext serverContext, bool throwWhenServerContextIsAllocated, string[] compareExchangeValues)
        {
            _database = database ?? throw new ArgumentNullException(nameof(database));
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
                IncludeUtil.GetDocIdFromInclude(document.Data, new StringSegment(include), _includedKeys, _database.IdentityPartsSeparator);
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
            {
                CreateServerContext();
            }

            var result = _database.ServerStore.Cluster.GetCompareExchangeValue(_serverContext, CompareExchangeKey.GetStorageKey(_database.Name, key));
            if (result.Index < 0)
                return false;

            Debug.Assert(ClusterWideTransactionHelper.IsAtomicGuardKey(key) == false || maxAtomicGuardIndex.HasValue);
            if (ClusterWideTransactionHelper.IsAtomicGuardKey(key) && maxAtomicGuardIndex.HasValue && result.Index > maxAtomicGuardIndex)
                return false; // we are seeing partially committed value, skip it
            
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

                if (TryGetCompareExchange(includedKey, maxAllowedAtomicGuardIndex, out var index, out var value) == false
                    && ClusterWideTransactionHelper.IsAtomicGuardKey(includedKey))
                    continue;  
                
                Results ??= new Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>>(StringComparer.OrdinalIgnoreCase);
                Results.Add(includedKey, new CompareExchangeValue<BlittableJsonReaderObject>(includedKey, index,  value));
            }
        }

        private void CreateServerContext()
        {
            if (_throwWhenServerContextIsAllocated)
                throw new InvalidOperationException("Cannot allocate new server context during materialization of compare exchange includes.");

            _releaseContext = _database.ServerStore.ContextPool.AllocateOperationContext(out _serverContext);
            _serverContext.OpenReadTransaction();
        }

        public void Dispose()
        {
            _releaseContext?.Dispose();
            _releaseContext = null;
        }
    }
}
