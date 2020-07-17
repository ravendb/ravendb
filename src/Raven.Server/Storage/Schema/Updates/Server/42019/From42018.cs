using System.Collections.Generic;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Json.Serialization;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Storage.Schema.Updates.Server
{
    public unsafe class From42018 : ISchemaUpdate
    {
        public int From => 42_018;

        public int To => 42_019;

        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Server;

        public bool Update(UpdateStep step)
        {
            var ids = new HashSet<long>();
            var minimal = long.MaxValue;
            const string dbKey = "db/";
            var continueAfterCommit = true;
            var skip = 0;

            while (continueAfterCommit)
            {
                continueAfterCommit = false;
                var fixedItems = 0;

                var items = step.WriteTx.OpenTable(ClusterStateMachine.ItemsSchema, ClusterStateMachine.Items);
                using (Slice.From(step.WriteTx.Allocator, dbKey, out Slice loweredPrefix))
                {
                    foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, 0))
                    {
                        var databaseName = ClusterStateMachine.GetCurrentItemKey(result.Value).Substring(3);
                        using (Slice.From(step.WriteTx.Allocator, dbKey + databaseName.ToLowerInvariant(), out var key))
                        {
                            if (items.VerifyKeyExists(key) == false)
                                continue;
                        }

                        using (Slice.From(step.WriteTx.Allocator, SubscriptionState.SubscriptionPrefix(databaseName), out var startWith))
                        using (var ctx = JsonOperationContext.ShortTermSingleUse())
                        {
                            foreach (var holder in items.SeekByPrimaryKeyPrefix(startWith, Slices.Empty, skip))
                            {
                                skip++;
                                var reader = holder.Value.Reader;
                                var ptr = reader.Read(2, out int size);
                                using (var doc = new BlittableJsonReaderObject(ptr, size, ctx))
                                {
                                    if (doc.TryGet(nameof(SubscriptionState.SubscriptionId), out long id) == false)
                                        continue;

                                    if (minimal > id)
                                        minimal = id;

                                    if (ids.Add(id))
                                        continue;

                                    minimal--;
                                    ids.Add(minimal);

                                    var subscriptionState = JsonDeserializationClient.SubscriptionState(doc);
                                    subscriptionState.SubscriptionId = minimal;
                                    var subscriptionItemName = SubscriptionState.GenerateSubscriptionItemKeyName(databaseName, subscriptionState.SubscriptionName);
                                    using (Slice.From(step.WriteTx.Allocator, subscriptionItemName, out Slice valueName))
                                    using (Slice.From(step.WriteTx.Allocator, subscriptionItemName.ToLowerInvariant(), out Slice valueNameLowered))
                                    using (var receivedSubscriptionState = ctx.ReadObject(subscriptionState.ToJson(), subscriptionState.SubscriptionName))
                                    {
                                        ClusterStateMachine.UpdateValue(0, items, valueNameLowered, valueName, receivedSubscriptionState);
                                    }
                                }

                                fixedItems++;
                                if (fixedItems < 1024)
                                    continue;

                                continueAfterCommit = true;
                                break;
                            }
                        }

                        if (continueAfterCommit)
                        {
                            break;
                        }
                    }
                }

                if (continueAfterCommit)
                {
                    step.Commit(null);
                    step.RenewTransactions();
                }
            }

            return true;
        }
    }
}
