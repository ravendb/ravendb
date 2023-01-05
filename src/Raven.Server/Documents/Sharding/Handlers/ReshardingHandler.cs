using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions.Database;
using Raven.Server.Documents.Operations;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json.Parsing;
using Sparrow.Server.Collections;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ReshardingHandler : ServerRequestHandler
    {
        [RavenAction("/admin/resharding/start", "GET", AuthorizationStatus.Operator)]
        public async Task StartResharding()
        {
            var database = GetStringQueryString("database");
            var buckets = GetStringValuesQueryString("bucket");
            var toShard = GetIntValueQueryString("to").Value;
            var raftId = GetRaftRequestIdFromQuery();

            using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            using (var raw = ServerStore.Cluster.ReadRawDatabaseRecord(context, database))
            {
                if (raw == null)
                    DatabaseDoesNotExistException.Throw(database);

                if (raw.IsSharded == false)
                    throw new InvalidOperationException($"{database} is not sharded");
            }

            var fromBucket = int.Parse(buckets[0]);
            var toBucket = int.Parse(buckets[1]);

            var id = ServerStore.Operations.GetNextOperationId();
            var opDesc = new ReshardingOperationDetails
            {
                FromBucket = fromBucket,
                ToBucket = toBucket,   
                ToShard = toShard
            };

            _ = ServerStore.Operations.AddLocalOperation(id, OperationType.Resharding, $"Move to shard {toShard} buckets [{fromBucket}-{toBucket}]", opDesc, async action =>
            {
                var result = new ReshardingResult();
                var messages = new AsyncQueue<string>();
                using (ServerStore.Sharding.RegisterForReshardingStatusUpdate(database, messages))
                {
                    var bucket = fromBucket;
                    while (ServerStore.ServerShutdown.IsCancellationRequested == false)
                    {
                        if (ServerStore.Sharding.HasActiveMigrations(database) == false)
                        {
                            if (bucket > toBucket)
                                break;

                            await ServerStore.Sharding.StartBucketMigration(database, bucket, toShard, $"{raftId ?? Guid.NewGuid().ToString()}/{bucket}");
                            bucket++;
                        }

                        var (success, message) = await messages.TryDequeueAsync(TimeSpan.FromSeconds(15));
                        result.Message = success ? message : "Waiting for new information...";
                        action?.Invoke(result);
                    }
                }
                result.Message = $"Finished resharding to shard {toShard} buckets [{fromBucket}-{toBucket}]";
                return result;
            });
        }

        public class ReshardingResult : IOperationResult, IOperationProgress
        {
            public string Message { get; set; }

            DynamicJsonValue IOperationResult.ToJson()
            {
                throw new NotImplementedException();
            }

            public IOperationProgress Clone()
            {
                throw new NotImplementedException();
            }

            public bool ShouldPersist => false;
            DynamicJsonValue IOperationProgress.ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Message)] = Message
                };
            }

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

        public class ReshardingOperationDetails : IOperationDetailedDescription
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
