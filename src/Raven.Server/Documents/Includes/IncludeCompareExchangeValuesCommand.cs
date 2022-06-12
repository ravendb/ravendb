using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Includes
{
    public class IncludeCompareExchangeValuesCommand : IDisposable
    {
        private readonly ServerStore _serverStore;
        private readonly string _databaseName;
        private readonly char _identityPartsSeparator;

        private readonly string[] _includes;

        private HashSet<string> _includedKeys;

        private IDisposable _releaseContext;
        private ClusterOperationContext _serverContext;
        private readonly bool _throwWhenServerContextIsAllocated;
        public Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>> Results;

        private long? _resultsEtag;

        private IncludeCompareExchangeValuesCommand([NotNull] DocumentDatabase database, ClusterOperationContext serverContext, bool throwWhenServerContextIsAllocated, string[] compareExchangeValues)
            : this(database.Name, database.ServerStore, database.IdentityPartsSeparator, serverContext, throwWhenServerContextIsAllocated, compareExchangeValues)
        {
        }

        private IncludeCompareExchangeValuesCommand([NotNull] ShardedDatabaseContext database, ClusterOperationContext serverContext, bool throwWhenServerContextIsAllocated, string[] compareExchangeValues)
            : this(database.DatabaseName, database.ServerStore, database.IdentityPartsSeparator, serverContext, throwWhenServerContextIsAllocated, compareExchangeValues)
        {
        }

        private IncludeCompareExchangeValuesCommand(string databaseName, ServerStore serverStore, char identityPartsSeparator, ClusterOperationContext serverContext, bool throwWhenServerContextIsAllocated, string[] compareExchangeValues)
        {
            _databaseName = databaseName;
            _identityPartsSeparator = identityPartsSeparator;
            _serverStore = serverStore;

            _serverContext = serverContext;
            _throwWhenServerContextIsAllocated = throwWhenServerContextIsAllocated;
            _includes = compareExchangeValues;
        }

        public static IncludeCompareExchangeValuesCommand ExternalScope(QueryOperationContext context, string[] compareExchangeValues)
        {
            return new IncludeCompareExchangeValuesCommand(context.Documents.DocumentDatabase, context.Server, throwWhenServerContextIsAllocated: true, compareExchangeValues);
        }

        public static IncludeCompareExchangeValuesCommand ExternalScope(ShardedDatabaseContext databaseContext, ClusterOperationContext serverContext, string[] compareExchangeValues)
        {
            return new IncludeCompareExchangeValuesCommand(databaseContext, serverContext, throwWhenServerContextIsAllocated: true, compareExchangeValues);
        }

        public static IncludeCompareExchangeValuesCommand InternalScope(DocumentDatabase database, string[] compareExchangeValues)
        {
            return new IncludeCompareExchangeValuesCommand(database, serverContext: null, throwWhenServerContextIsAllocated: false, compareExchangeValues);
        }

        public static IncludeCompareExchangeValuesCommand InternalScope(ShardedDatabaseContext databaseContext, string[] compareExchangeValues)
        {
            return new IncludeCompareExchangeValuesCommand(databaseContext, serverContext: null, throwWhenServerContextIsAllocated: false, compareExchangeValues);
        }

        internal long ResultsEtag
        {
            get
            {
                if (_resultsEtag == null)
                {
                    if (_serverContext == null)
                        throw new InvalidOperationException($"Execute '{nameof(Materialize)}' method first.");

                    _resultsEtag = _serverStore.Cluster.GetLastCompareExchangeIndexForDatabase(_serverContext, _databaseName);
                }

                return _resultsEtag.Value;
            }
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
            Gather(document?.Data);
        }

        internal void Gather(BlittableJsonReaderObject document)
        {
            if (document == null)
                return;

            if (_includes == null || _includes.Length == 0)
                return;

            _includedKeys ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var include in _includes)
                IncludeUtil.GetDocIdFromInclude(document, new StringSegment(include), _includedKeys, _identityPartsSeparator);
        }

        internal void Materialize()
        {
            if (_includedKeys == null || _includedKeys.Count == 0)
                return;

            if (_serverContext == null)
            {
                if (_throwWhenServerContextIsAllocated)
                    throw new InvalidOperationException("Cannot allocate new server context during materialization of compare exchange includes.");

                _releaseContext = _serverStore.Engine.ContextPool.AllocateOperationContext(out _serverContext);
                _serverContext.OpenReadTransaction();
            }

            foreach (var includedKey in _includedKeys)
            {
                if (string.IsNullOrEmpty(includedKey))
                    continue;

                var value = _serverStore.Cluster.GetCompareExchangeValue(_serverContext, CompareExchangeKey.GetStorageKey(_databaseName, includedKey));

                if (Results == null)
                    Results = new Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>>(StringComparer.OrdinalIgnoreCase);

                Results.Add(includedKey, new CompareExchangeValue<BlittableJsonReaderObject>(includedKey, value.Index, value.Value));
            }
        }

        public void Dispose()
        {
            _releaseContext?.Dispose();
            _releaseContext = null;
        }
    }
}
