using System;
using System.Collections.Generic;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Sharding;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Commands.Sharding
{
    public sealed class StartBucketMigrationCommand : UpdateDatabaseCommand
    {
        public int? SourceShard;
        public int DestinationShard;
        public int Bucket;
        public string Prefix;

        private ShardBucketMigration _migration;

        public StartBucketMigrationCommand()
        {
        }

        public StartBucketMigrationCommand(int bucket, int destShard, string database, string prefix, string raftId) : base(database, raftId)
        {
            if (bucket >= ShardHelper.NumberOfBuckets && string.IsNullOrEmpty(prefix))
                throw new InvalidOperationException($"Bucket {bucket} belongs to a prefixed range, but 'prefix' parameter wasn't provided");

            Bucket = bucket;
            DestinationShard = destShard;
            Prefix = prefix;
        }

        public StartBucketMigrationCommand(int bucket, int sourceShard, int destShard, string database, string raftId) : this(bucket, destShard, database, prefix: null, raftId)
        {
            SourceShard = sourceShard;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            var sourceShard = SourceShard ?? ShardHelper.GetShardNumberFor(record.Sharding, Bucket);
            if (sourceShard == DestinationShard)
                return; // nothing to do

            if (record.Sharding.BucketMigrations.Count > 0)
            {
                foreach (var migration in record.Sharding.BucketMigrations)
                {
                    if (migration.Value.IsActive)
                        throw new RachisApplyException(
                            $"Only one bucket can be transferred at a time, currently bucket {migration.Key} is {migration.Value.Status}");

                    if (migration.Key == Bucket)
                        throw new RachisApplyException($"Can't migrate bucket {Bucket}, since it is still migrating.");
                }
            }

            AssertDestinationShardExists(record.Sharding);

            _migration = new ShardBucketMigration
            {
                Bucket = Bucket,
                DestinationShard = DestinationShard,
                SourceShard = sourceShard,
                MigrationIndex = etag,
                Status = MigrationStatus.Moving
            };

            record.Sharding.BucketMigrations.Add(Bucket, _migration);
        }

        public override void AfterDatabaseRecordUpdate(ClusterOperationContext ctx, Table items, Logger clusterAuditLog)
        {
            if (_migration == null)
                return;

            ProcessSubscriptionsForMigration(ctx, _migration);
        }

        private void ProcessSubscriptionsForMigration(ClusterOperationContext context, ShardBucketMigration migration)
        {
            var index = migration.MigrationIndex;
            var database = ShardHelper.ToShardName(DatabaseName, migration.SourceShard);

            var updatedSubscriptionStates = new List<(SubscriptionState State, Slice Key)>(); 

            foreach (var (key, blittableState) in ClusterStateMachine.ReadValuesStartingWith(context, SubscriptionState.SubscriptionPrefix(DatabaseName)))
            {
                var state = JsonDeserializationClient.SubscriptionState(blittableState);
                if (state.ShardingState.ChangeVectorForNextBatchStartingPointPerShard.TryGetValue(database, out var changeVector) == false)
                {
                    changeVector = string.Empty;
                }

                if (state.ShardingState.ProcessedChangeVectorPerBucket.ContainsKey(migration.Bucket) == false)
                {
                    state.ShardingState.ProcessedChangeVectorPerBucket[migration.Bucket] = changeVector;
                }

                updatedSubscriptionStates.Add((state, key.Clone(context.Allocator)));
            }

            foreach (var states in updatedSubscriptionStates)
            {
                var state = states.State;
                var key = states.Key;

                using (Slice.From(context.Allocator, state.SubscriptionName, out Slice valueName))
                using (var updated = context.ReadObject(state.ToJson(), "migration"))
                {
                    ClusterStateMachine.UpdateValueForItemsTable(context, index, key, valueName, updated);
                }
            }
        }

        private void AssertDestinationShardExists(ShardingConfiguration shardingConfiguration)
        {
            if (shardingConfiguration.Shards.ContainsKey(DestinationShard) == false)
                throw new RachisApplyException($"Database '{DatabaseName}' : Failed to start migration of bucket '{Bucket}'. Destination shard {DestinationShard} doesn't exist");

            if (string.IsNullOrEmpty(Prefix)) 
                return;

            // prefixed bucket range
            var index = shardingConfiguration.Prefixed.BinarySearch(new PrefixedShardingSetting(Prefix), PrefixedSettingComparer.Instance);
            if (index < 0)
                throw new RachisApplyException($"Database '{DatabaseName}' : Failed to start migration of bucket '{Bucket}'. Prefix {Prefix} doesn't exist");

            var shards = shardingConfiguration.Prefixed[index].Shards;
            if (shards == null || shards.Contains(DestinationShard) == false)
                throw new RachisApplyException($"Database '{DatabaseName}' : Failed to start migration of bucket '{Bucket}'. Destination shard {DestinationShard} doesn't exist");
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(SourceShard)] = SourceShard;
            json[nameof(DestinationShard)] = DestinationShard;
            json[nameof(Bucket)] = Bucket;
            json[nameof(Prefix)] = Prefix;
        }
    }
}
