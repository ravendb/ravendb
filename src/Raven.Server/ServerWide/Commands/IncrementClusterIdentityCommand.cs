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

        public override void Execute(TransactionOperationContext context, Table items, long index, DatabaseRecord record, RachisState state, out object result)
        {
            var identities = context.Transaction.InnerTransaction.ReadTree(ClusterStateMachine.Identities);
            var itemKey = GetItemId();

            using (Slice.From(context.Allocator, itemKey, out var key))
            {
                result = identities.Increment(key, 1);
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(DatabaseName)] = DatabaseName;
            json[nameof(Prefix)] = Prefix;
        }
    }
}
