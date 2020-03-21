using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.CompareExchange;
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

        public List<CompareExchangeValue<BlittableJsonReaderObject>> Results;

        public IncludeCompareExchangeValuesCommand(DocumentDatabase database, string[] compareExchangeValues)
        {
            _database = database ?? throw new ArgumentNullException(nameof(database));
            _includes = compareExchangeValues;
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

            _releaseContext = _database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context);

            context.OpenReadTransaction(); // we will release it on dispose

            foreach (var includedKey in _includedKeys)
            {
                if (string.IsNullOrEmpty(includedKey))
                    continue;

                var value = _database.ServerStore.Cluster.GetCompareExchangeValue(context, CompareExchangeKey.GetStorageKey(_database.Name, includedKey));

                if (Results == null)
                    Results = new List<CompareExchangeValue<BlittableJsonReaderObject>>();

                Results.Add(new CompareExchangeValue<BlittableJsonReaderObject>(includedKey, value.Index, value.Value));
            }
        }

        public void Dispose()
        {
            _releaseContext?.Dispose();
            _releaseContext = null;
        }
    }
}
