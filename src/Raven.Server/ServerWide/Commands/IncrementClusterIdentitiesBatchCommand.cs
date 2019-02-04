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

        public override unsafe void Execute(TransactionOperationContext context, Table items, long index, DatabaseRecord record, RachisState state, out object result)
        {
            var identitiesItems = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.IdentitiesSchema, ClusterStateMachine.Identities);
            var listResult = new List<long>();
            foreach (var identity in Identities)
            {
                CompareExchangeCommandBase.GetKeyAndPrefixIndexSlices(context.Allocator, DatabaseName, identity, index, out var keyTuple, out var indexTuple);

                using (keyTuple.Scope)
                using (indexTuple.Scope)
                using (Slice.External(context.Allocator, keyTuple.Buffer.Ptr, keyTuple.Buffer.Length, out var keySlice))
                using (Slice.External(context.Allocator, indexTuple.Buffer.Ptr, indexTuple.Buffer.Length, out var prefixIndexSlice))
                {
                    long value;
                    if (identitiesItems.ReadByKey(keySlice, out var reader))
                    {
                        value = GetValue(reader);
                        value += 1;
                    }
                    else
                        value = 1;

                    UpdateTableRow(index, identitiesItems, value, keySlice, prefixIndexSlice);

                    listResult.Add(value);
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
