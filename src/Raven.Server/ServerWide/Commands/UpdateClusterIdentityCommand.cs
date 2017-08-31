using System;
using System.Collections.Generic;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Commands
{
    public class UpdateClusterIdentityCommand : UpdateValueForDatabaseCommand
    {
        public Dictionary<string, long> Identities { get; set; }

        public UpdateClusterIdentityCommand()
            : base(null)
        {
        }

        public UpdateClusterIdentityCommand(string databaseName, Dictionary<string, long> identities)
            : base(databaseName)
        {
            Identities = new Dictionary<string, long>(identities);
        }

        public override string GetItemId()
        {
            throw new NotSupportedException();
        }

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, DatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue, bool isPassive)
        {
            throw new NotSupportedException();
        }

        public override void Execute(TransactionOperationContext context, Table items, long index, DatabaseRecord record, bool isPassive, out object result)
        {
            result = null;
            var identities = context.Transaction.InnerTransaction.ReadTree(ClusterStateMachine.Identities);

            foreach (var kvp in Identities)
            {
                var itemKey = IncrementClusterIdentityCommand.GetStorageKey(DatabaseName, kvp.Key);

                using (Slice.From(context.Allocator, itemKey, out var key))
                {
                    identities.AddMax(key, kvp.Value);
                }
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Identities)] = (Identities ?? new Dictionary<string, long>()).ToJson();
        }
    }
}
