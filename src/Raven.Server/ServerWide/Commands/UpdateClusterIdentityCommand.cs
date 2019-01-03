using System;
using System.Collections.Generic;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
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

        public UpdateClusterIdentityCommand(string databaseName, Dictionary<string, long> identities, bool force)
            : base(databaseName)
        {
            Identities = new Dictionary<string, long>(identities);
            Force = force;
        }

        public bool Force { get; set; }

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
                    bool isSet;
                    if (Force == false)
                    {
                        isSet = identities.AddMax(key, kvp.Value);                        
                    }
                    else
                    {
                        identities.Add(key, kvp.Value);
                        isSet = true;
                    }
                    
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

                    resultDict.TryGetValue(keyString, out var oldVal);
                    resultDict[keyString] = Math.Max(oldVal, newVal);
                }
            }

            result = resultDict;
        }

        public override object FromRemote(object remoteResult)
        {
            var bja = remoteResult as BlittableJsonReaderArray;
            if (bja == null)
                throw new RachisApplyException($"UpdateClusterIdentityCommand.FromRemote expected an object of type 'BlittableJsonReaderArray' but got {remoteResult.GetType().Name}.");

            var res = new Dictionary<string, long>();
            foreach (var o in bja)
            {
                var bjro = o as BlittableJsonReaderObject;
                if (bjro == null)
                    throw new RachisApplyException($"UpdateClusterIdentityCommand.FromRemote expected an array of type 'BlittableJsonReaderObject' but got {o.GetType().Name}.");

                var names = bjro.GetPropertyNames();
                foreach (var name in names)
                {
                    if (bjro.TryGetMember(name, out var value))
                    {
                        if (value is long == false)
                        {
                            throw new RachisApplyException($"UpdateClusterIdentityCommand.FromRemote expected properties with 'long' type but got {value.GetType().Name}.");
                        }

                        res.Add(name,(long)value);
                    }
                }
            }

            return res;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Identities)] = (Identities ?? new Dictionary<string, long>()).ToJson();
            if (Force)
            {
                json[nameof(Force)] = true;
            }
        }
    }
}
