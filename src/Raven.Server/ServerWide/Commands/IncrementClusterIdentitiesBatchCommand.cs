using System;
using System.Collections.Generic;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Commands
{
    public class IncrementClusterIdentitiesBatchCommand : UpdateValueForDatabaseCommand
    {
        public List<string> Identities;

        public IncrementClusterIdentitiesBatchCommand()
            : base(null)
        {
            // for deserialization
        }

        public IncrementClusterIdentitiesBatchCommand(string databaseName, List<string> identities) : base(databaseName)
        {
            Identities = identities;
            DatabaseName = databaseName;
        }

        public override void Execute(TransactionOperationContext context, Table items, long index, DatabaseRecord record, RachisState state, out object result)
        {
            var identitiesTree = context.Transaction.InnerTransaction.ReadTree(ClusterStateMachine.Identities);
            var listResult = new List<long>();
            foreach (var identity in Identities)
            {
                using (Slice.From(context.Allocator, GetStorageKey(DatabaseName, identity), out var key))
                {
                    var newVal = identitiesTree.Increment(key, 1);
                    // we assume this is single thread task and therefor we return the first identity of each id. 
                    // The 'client' of this task sent amount of each id, and therefor the created identities are first identity to first + amount
                    listResult.Add(newVal);
                }
            }

            result = listResult;
        }

        public override string GetItemId()
        {
            throw new NotImplementedException();
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(DatabaseName)] = DatabaseName;
            json[nameof(Identities)] = new DynamicJsonArray(Identities);
        }

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, DatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue, RachisState state)
        {
            throw new NotImplementedException();
        }

        public override object FromRemote(object remoteResult)
        {
            var rc = new List<long>();
            var obj = remoteResult as BlittableJsonReaderArray;

            if (obj == null)
            {
                // this is an error as we expect BlittableJsonReaderArray, but we will pass the object value to get later appropriate exception
                return base.FromRemote(remoteResult);
            }

            foreach (var o in obj)
            {
                rc.Add((long)o);
            }
            return rc;
        }
    }
}
