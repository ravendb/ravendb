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

        public UpdateClusterIdentityCommand(string databaseName, Dictionary<string, long> identities, bool force, bool fromBackup = false)
            : base(databaseName)
        {
            Identities = new Dictionary<string, long>(identities);
            Force = force;
            FromBackup = fromBackup;
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

        public override unsafe void Execute(TransactionOperationContext context, Table items, long index, DatabaseRecord record, RachisState state, out object result)
        {
            var resultDict = new Dictionary<string, long>();
            var identitiesItems = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.IdentitiesSchema, ClusterStateMachine.Identities);

            foreach (var kvp in Identities)
            {
                CompareExchangeCommandBase.GetKeyAndPrefixIndexSlices(context.Allocator, DatabaseName, kvp.Key, index, out var keyTuple, out var indexTuple);

                using (keyTuple.Scope)
                using (indexTuple.Scope)
                using (Slice.External(context.Allocator, keyTuple.Buffer.Ptr, keyTuple.Buffer.Length, out var keySlice))
                using (Slice.External(context.Allocator, indexTuple.Buffer.Ptr, indexTuple.Buffer.Length, out var prefixIndexSlice))
                {
                    bool isSet;
                    if (Force == false)
                    {
                        isSet = false;
                        if (identitiesItems.SeekOnePrimaryKeyPrefix(keySlice, out var tvr))
                        {
                            var value = GetValue(tvr);
                            if (value < kvp.Value)
                                isSet = true;
                        }
                        else
                        {
                            using (identitiesItems.Allocate(out var tvb))
                            {
                                tvb.Add(keySlice);
                                tvb.Add(kvp.Value);
                                tvb.Add(index);
                                tvb.Add(prefixIndexSlice);

                                identitiesItems.Set(tvb);
                            }
                        }

                    }
                    else
                        isSet = true;

                    var keyString = keySlice.ToString().ToLowerInvariant();
                    resultDict.TryGetValue(keyString, out var oldVal);
                    long newVar;

                    if (isSet)
                    {
                        UpdateTableRow(index, identitiesItems, kvp.Value, keySlice, prefixIndexSlice);
                        newVar = kvp.Value;
                    }
                    else
                    {
                        identitiesItems.SeekOnePrimaryKeyPrefix(keySlice, out var tvr);
                        newVar = GetValue(tvr);
                    }

                    resultDict[keyString] = Math.Max(oldVal, newVar);
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
            json[nameof(FromBackup)] = FromBackup;
            if (Force)
            {
                json[nameof(Force)] = true;
            }
        }
    }
}
