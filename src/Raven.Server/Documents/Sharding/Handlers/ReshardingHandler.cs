using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Database;
using Raven.Server.Documents.Operations;
using Raven.Server.Json;
using Raven.Server.Rachis;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Collections;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public sealed class ReshardingHandler : ServerRequestHandler
    {
        [RavenAction("/admin/resharding/start", "POST", AuthorizationStatus.Operator)]
        public async Task StartResharding()
        {
            var database = GetStringQueryString("database");
            var fromBucket = GetIntValueQueryString("fromBucket").Value;
            var toBucket = GetIntValueQueryString("toBucket").Value;
            var toShard = GetIntValueQueryString("toShard").Value;
            var raftId = GetRaftRequestIdFromQuery();

            if (fromBucket > toBucket || fromBucket < 0 || toBucket < 0)
                throw new ArgumentException($"Invalid buckets range [{fromBucket}-{toBucket}]");

            using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            using (var raw = ServerStore.Cluster.ReadRawDatabaseRecord(context, database))
            {
                if (raw == null)
                    DatabaseDoesNotExistException.Throw(database);

                if (raw.IsSharded == false)
                    throw new InvalidOperationException($"{database} is not sharded");
            }

            var operationId = ServerStore.Operations.GetNextOperationId();
            var opDesc = new ReshardingOperationDetails
            {
                FromBucket = fromBucket,
                ToBucket = toBucket,
                ToShard = toShard
            };
            var token = CreateBackgroundOperationToken();
            _ = ServerStore.Operations.AddLocalOperation(operationId, OperationType.Resharding, $"Move to shard {toShard} buckets [{fromBucket}-{toBucket}]", opDesc, async action =>
            {
                var result = new ReshardingResult();
                var messages = new AsyncQueue<string>(token.Token);
                using (ServerStore.Sharding.RegisterForReshardingStatusUpdate(database, messages))
                {
                    var bucket = fromBucket;
                    while (ServerStore.ServerShutdown.IsCancellationRequested == false)
                    {
                        token.ThrowIfCancellationRequested();

                        if (ServerStore.Sharding.HasActiveMigrations(database) == false)
                        {
                            if (bucket > toBucket)
                                break;

                            var (index, _) = await ServerStore.Sharding.StartBucketMigration(database, bucket, toShard,
                                $"{raftId ?? Guid.NewGuid().ToString()}/{bucket}");
                            await ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, index, token.Token);

                            bucket++;
                        }

                        var (success, message) = await messages.TryDequeueAsync(TimeSpan.FromSeconds(15));
                        result.Message = success ? message : "Waiting for new information...";
                        action?.Invoke(result);
                    }
                }
                result.Message = $"Finished resharding to shard {toShard} buckets [{fromBucket}-{toBucket}]";
                return result;
            }, token: token);

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
            }
        }

        [RavenAction("/debug/sharding/find/bucket", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetBucket()
        {
            var id = GetStringQueryString("id");
            var database = GetStringQueryString("name", required: false);

            using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            {
                int? bucket = default;
                int? shard = default;
                if (database != null)
                {
                    using (context.OpenReadTransaction())
                    using (var raw = ServerStore.Cluster.ReadRawDatabaseRecord(context, database))
                    {
                        if (raw == null)
                            DatabaseDoesNotExistException.Throw(database);

                        if (raw.IsSharded == false)
                            throw new InvalidOperationException($"{database} is not sharded");

                        bucket = ShardHelper.GetBucketFor(raw.Sharding.MaterializedConfiguration, context.Allocator, id);
                        shard = ShardHelper.GetShardNumberFor(raw.Sharding, context, id);
                    }
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName("Bucket");
                    writer.WriteInteger(bucket.Value);

                    writer.WriteComma();

                    writer.WritePropertyName("Shard");
                    writer.WriteInteger(shard.Value);

                    writer.WriteEndObject();
                }
            }
        }

        public sealed class ReshardingResult : IOperationResult, IOperationProgress
        {
            public string Message { get; set; }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Message)] = Message
                };
            }

            public IOperationProgress Clone()
            {
                throw new NotImplementedException();
            }

            public bool ShouldPersist => false;

            public bool CanMerge => false;
            public void MergeWith(IOperationProgress progress)
            {
                throw new NotImplementedException();
            }

            public void MergeWith(IOperationResult result)
            {
                throw new NotImplementedException();
            }
        }

        public sealed class ReshardingOperationDetails : IOperationDetailedDescription
        {
            public int ToShard { get; set; }
            public int FromBucket { get; set; }
            public int ToBucket { get; set; }

            DynamicJsonValue IOperationDetailedDescription.ToJson()
            {
                return new DynamicJsonValue(GetType())
                {
                    [nameof(ToShard)] = ToShard,
                    [nameof(FromBucket)] = FromBucket,
                    [nameof(ToBucket)] = ToBucket
                };
            }
        }
    }
}
