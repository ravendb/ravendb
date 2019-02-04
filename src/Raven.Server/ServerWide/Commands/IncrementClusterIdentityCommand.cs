using System;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Commands
{
    public class IncrementClusterIdentityCommand : UpdateValueForDatabaseCommand
    {
        private string _itemId;

        public string Prefix { get; set; }

        public IncrementClusterIdentityCommand()
            : base(null)
        {
            // for deserialization
        }

        public IncrementClusterIdentityCommand(string databaseName, string prefix)
            : base(databaseName)
        {
            Prefix = prefix;
        }

        public override string GetItemId()
        {
            return _itemId ?? (_itemId = GetStorageKey(DatabaseName, Prefix));
        }

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, DatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue, RachisState state)
        {
            throw new NotImplementedException();
        }

        public override unsafe void Execute(TransactionOperationContext context, Table items, long index, DatabaseRecord record, RachisState state, out object result)
        {
            var identitiesItems = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.IdentitiesSchema, ClusterStateMachine.Identities);

            CompareExchangeCommandBase.GetKeyAndPrefixIndexSlices(context.Allocator, DatabaseName, Prefix, index, out var keyTuple, out var indexTuple);

            using (keyTuple.Scope)
            using (indexTuple.Scope)
            using (Slice.External(context.Allocator, keyTuple.Buffer.Ptr, keyTuple.Buffer.Length, out var keySlice))
            using (Slice.External(context.Allocator, indexTuple.Buffer.Ptr, indexTuple.Buffer.Length, out var prefixIndexSlice))
            {
                if(identitiesItems.SeekOnePrimaryKeyPrefix(keySlice, out var entry))
                {
                    var value = GetValue(entry);
                    value++;
                    UpdateTableRow(index, identitiesItems, value, keySlice, prefixIndexSlice);
                    result = value;
                }
                else
                    result = null;
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(DatabaseName)] = DatabaseName;
            json[nameof(Prefix)] = Prefix;
        }
    }
}
