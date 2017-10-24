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

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, DatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue, RachisState state)
        {
            throw new NotSupportedException();
        }

        public override void Execute(TransactionOperationContext context, Table items, long index, DatabaseRecord record, RachisState state, out object result)
        {
            var resultDict = new Dictionary<string, long>();
            var identities = context.Transaction.InnerTransaction.ReadTree(ClusterStateMachine.Identities);

            foreach (var kvp in Identities)
            {
                var itemKey = GetStorageKey(DatabaseName, kvp.Key);

                using (Slice.From(context.Allocator, itemKey, out var key))
                {
                    var isSet = identities.AddMax(key, kvp.Value);
                    long newVal;
                    if (isSet)
                    {
                        newVal = kvp.Value;
                    }
                    else
                    {
                        var rc = identities.ReadLong(key);
                        newVal = rc ?? -1; // '-1' should not happen
                    }

                    var keyString = key.ToString().ToLowerInvariant();

                    if (resultDict.TryAdd(keyString, newVal) == false)
                    {
                        if (newVal > resultDict[keyString])
                            resultDict[keyString] = newVal;
                    }

                    if (resultDict.TryGetValue(keyString, out var oldVal) == false)
                    {
                        resultDict.Add(keyString, newVal);
                    }
                    else if (newVal > oldVal)
                    {
                        resultDict[keyString] = newVal;
                    }
                }
            }

            result = resultDict;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Identities)] = (Identities ?? new Dictionary<string, long>()).ToJson();
        }
    }
}
