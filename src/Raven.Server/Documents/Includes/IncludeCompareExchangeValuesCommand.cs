using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.CompareExchange;
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

        public Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>> Results;

        private IncludeCompareExchangeValuesCommand(DocumentDatabase database, TransactionOperationContext serverContext, string[] compareExchangeValues)
        {
            _database = database ?? throw new ArgumentNullException(nameof(database));
            _serverContext = serverContext;
            _includes = compareExchangeValues;
        }

        public static IncludeCompareExchangeValuesCommand ExternalScope(QueryOperationContext context, string[] compareExchangeValues)
        {
            return new IncludeCompareExchangeValuesCommand(context.Documents.DocumentDatabase, context.Server, compareExchangeValues);
        }

        public static IncludeCompareExchangeValuesCommand InternalScope(DocumentDatabase database, string[] compareExchangeValues)
        {
            return new IncludeCompareExchangeValuesCommand(database, serverContext: null, compareExchangeValues);
        }

        internal void Gather(Document document)
        {
            if (document == null)
                return;

            if (_includes == null || _includes.Length == 0)
                return;

            if (_includedKeys == null)
                _includedKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var include in _includes)
                IncludeUtil.GetDocIdFromInclude(document.Data, new StringSegment(include), _includedKeys, _database.IdentityPartsSeparator);
        }

        internal void Materialize()
        {
            if (_includedKeys == null || _includedKeys.Count == 0)
                return;

            if (_serverContext == null)
            {
                _releaseContext = _database.ServerStore.ContextPool.AllocateOperationContext(out _serverContext);
                _serverContext.OpenReadTransaction();
            }

            foreach (var includedKey in _includedKeys)
            {
                if (string.IsNullOrEmpty(includedKey))
                    continue;

                var value = _database.ServerStore.Cluster.GetCompareExchangeValue(_serverContext, CompareExchangeKey.GetStorageKey(_database.Name, includedKey));

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
