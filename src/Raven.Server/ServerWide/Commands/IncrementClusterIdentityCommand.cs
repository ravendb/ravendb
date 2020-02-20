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
        {
            // for deserialization
        }

        public IncrementClusterIdentityCommand(string databaseName, string prefix, string uniqueRequestId)
            : base(databaseName, uniqueRequestId)
        {
            Prefix = prefix;
        }

        public override string GetItemId()
        {
            return _itemId ?? (_itemId = GetStorageKey(DatabaseName, Prefix));
        }

        public static string GetStorageKey(string databaseName, string prefix)
        {
            return $"{databaseName.ToLowerInvariant()}/{prefix?.ToLowerInvariant()}";
        }

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, DatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue)
        {
            throw new NotImplementedException();
        }

        public override unsafe void Execute(ClusterOperationContext context, Table items, long index, DatabaseRecord record, RachisState state, out object result)
        {
            var identitiesItems = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.IdentitiesSchema, ClusterStateMachine.Identities);

            CompareExchangeCommandBase.GetKeyAndPrefixIndexSlices(context.Allocator, DatabaseName, Prefix, index, out var keyTuple, out var indexTuple);

            using (keyTuple.Scope)
            using (indexTuple.Scope)
            using (Slice.External(context.Allocator, keyTuple.Buffer.Ptr, keyTuple.Buffer.Length, out var keySlice))
            using (Slice.External(context.Allocator, indexTuple.Buffer.Ptr, indexTuple.Buffer.Length, out var prefixIndexSlice))
            {
                long value;
                if (identitiesItems.SeekOnePrimaryKeyPrefix(keySlice, out var entry))
                {
                    value = GetValue(entry);
                    value++;
                }
                else
                    value = 1;

                UpdateTableRow(index, identitiesItems, value, keySlice, prefixIndexSlice);
                result = value;
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(DatabaseName)] = DatabaseName;
            json[nameof(Prefix)] = Prefix;
        }
    }
}
