using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Sharding.Handlers.Processors.Subscriptions;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedSubscriptionsHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/subscriptions", "PUT")]
        public async Task Create()
        {
            using (var processor = new ShardedSubscriptionsHandlerProcessorForPutSubscription(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/subscriptions/update", "POST")]
        public async Task Update()
        {
            using (var processor = new ShardedSubscriptionsHandlerProcessorForPostSubscription(this))
                await processor.ExecuteAsync();
        }
        
        public async Task CreateSubscriptionInternalAsync(BlittableJsonReaderObject bjro, long? id, bool? disabled, SubscriptionCreationOptions options, JsonOperationContext context)
        {
            if (TrafficWatchManager.HasRegisteredClients)
                AddStringToHttpContext(bjro.ToString(), TrafficWatchChangeType.Subscriptions);

            var sub = SubscriptionConnection.ParseSubscriptionQuery(options.Query);
            var changeVectorValidationResult = await TryValidateChangeVector(options, sub);

            var (etag, _) = await ServerStore.SendToLeaderAsync(new PutShardedSubscriptionCommand(DatabaseContext.DatabaseName, options.Query, options.MentorNode, GetRaftRequestIdFromQuery())
            {
                InitialChangeVectorPerShard = changeVectorValidationResult.ChangeVectorsCollection,
                InitialChangeVector = changeVectorValidationResult.InitialChangeVector,
                SubscriptionName = options.Name,
                SubscriptionId = id,
                Disabled = disabled ?? false
            });

            await ServerStore.Cluster.WaitForIndexNotification(etag, ServerStore.Engine.OperationTimeout);

            long subscriptionId;
            long index;
            if (id != null)
            {
                // updated existing subscription
                subscriptionId = id.Value;
                index = etag;

            }
            else
            {
                subscriptionId = etag;
                index = etag;
            }

            var name = options.Name ?? subscriptionId.ToString();

            await GetResponsibleNodesAndWaitForExecution(name, index);

            HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    [nameof(CreateSubscriptionResult.Name)] = name,
                    [nameof(CreateSubscriptionResult.RaftCommandIndex)] = index
                });
            }
        }

        private struct ChangeVectorValidationResult
        {
            public Dictionary<string, string> ChangeVectorsCollection;
            public string InitialChangeVector;
        }

        private async Task<ChangeVectorValidationResult> TryValidateChangeVector(SubscriptionCreationOptions options, SubscriptionConnection.ParsedSubscription sub)
        {
            var result = new ChangeVectorValidationResult()
            {
                ChangeVectorsCollection = null,
                InitialChangeVector = null
            };
            if (Enum.TryParse(options.ChangeVector, out Client.Constants.Documents.SubscriptionChangeVectorSpecialStates changeVectorSpecialValue))
            {
                switch (changeVectorSpecialValue)
                {
                    case Client.Constants.Documents.SubscriptionChangeVectorSpecialStates.BeginningOfTime:
                        break;
                    case Client.Constants.Documents.SubscriptionChangeVectorSpecialStates.LastDocument:
                        result.ChangeVectorsCollection = (await ShardExecutor.ExecuteParallelForAllAsync(new ShardedLastChangeVectorForCollectionOperation(this, sub.Collection, DatabaseContext.DatabaseName))).LastChangeVectors;
                        foreach ((string key, string value) in result.ChangeVectorsCollection)
                        {
                            try
                            {
                                value.ToChangeVector();
                            }
                            catch (Exception e)
                            {
                                throw new InvalidOperationException($"Could not parse change vector for shard '{key}', CV: '{value}'", e);
                            }
                        }

                        break;
                    case Client.Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange:
                        result.InitialChangeVector = options.ChangeVector;
                        break;
                    default:
                        result.InitialChangeVector = options.ChangeVector;
                        if (string.IsNullOrEmpty(result.InitialChangeVector) == false)
                        {
                            throw new InvalidOperationException("Setting initial change vector for sharded subscription is not allowed.");
                        }

                        break;
                }
            }

            return result;
        }

        private async Task GetResponsibleNodesAndWaitForExecution(string name, long index)
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var subscription = ServerStore.Cluster.Subscriptions.ReadSubscriptionStateByName(context, DatabaseContext.DatabaseName, name);

                foreach (var topology in DatabaseContext.ShardsTopology)
                {
                    var node = topology.WhoseTaskIsIt(ServerStore.Engine.CurrentState, subscription, null);
                    if (node == null || node == ServerStore.NodeTag)
                        continue;

                    await WaitForExecutionOnSpecificNode(context, ServerStore.GetClusterTopology(context), node, index);
                }
            }
        }

        [RavenShardedAction("/databases/*/subscriptions", "DELETE")]
        public async Task Delete()
        {
            using (var processor = new ShardedSubscriptionHandlerProcessorForDeleteSubscription(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/subscriptions/state", "GET")]
        public async Task GetSubscriptionState()
        {
            using (var processor = new ShardedSubscriptionsHandlerProcessorForGetSubscriptionState(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/subscriptions", "GET")]
        public async Task GetAll()
        {
            using (var processor = new ShardedSubscriptionsHandlerProcessorForGetSubscription(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/subscriptions/drop", "POST")]
        public async Task DropSubscriptionConnection()
        {
            using (var processor = new ShardedSubscriptionsHandlerProcessorForDropSubscriptionConnection(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/subscriptions/connection-details", "GET")]
        public async Task GetSubscriptionConnectionDetails()
        {
            using (var processor = new ShardedSubscriptionsHandlerProcessorForGetConnectionDetails(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/subscriptions/try", "POST")]
        public async Task Try()
        {
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                using var json = await context.ReadForMemoryAsync(RequestBodyStream(), null);
                var tryout = JsonDeserializationServer.SubscriptionTryout(json);

                var sub = SubscriptionConnection.ParseSubscriptionQuery(tryout.Query);
                if (sub.Collection == null)
                    throw new ArgumentException("Collection must be specified");

                const int maxPageSize = 1024;
                var pageSize = GetIntValueQueryString("pageSize") ?? 1;
                if (pageSize > maxPageSize)
                    throw new ArgumentException($"Cannot gather more than {maxPageSize} results during tryouts, but requested number was {pageSize}.");

                var timeLimit = GetIntValueQueryString("timeLimit", false);
                var result = await ShardExecutor.ExecuteParallelForAllAsync(new ShardedSubscriptionTryoutOperation(HttpContext, context, tryout, pageSize, timeLimit));
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Results");
                    writer.WriteStartArray();
                    var numberOfDocs = 0;
                    var f = true;

                    foreach (var res in result.Results)
                    {
                        if (numberOfDocs == pageSize)
                            break;

                        if (res is not BlittableJsonReaderObject bjro)
                            continue;

                        using (bjro)
                        {
                            if (f == false)
                                writer.WriteComma();

                            f = false;
                            WriteBlittable(bjro, writer);
                            numberOfDocs++;
                        }
                    }

                    writer.WriteEndArray();
                    writer.WriteComma();
                    writer.WritePropertyName("Includes");
                    DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Egor, DevelopmentHelper.Severity.Major, "https://issues.hibernatingrhinos.com/issue/RavenDB-16279");
                    writer.WriteStartObject();
                    writer.WriteEndObject();
                    writer.WriteEndObject();

                }
            }
        }

        private static unsafe void WriteBlittable(BlittableJsonReaderObject bjro, AsyncBlittableJsonTextWriter writer)
        {
            var first = true;

            var prop = new BlittableJsonReaderObject.PropertyDetails();
            writer.WriteStartObject();
            using (var buffers = bjro.GetPropertiesByInsertionOrder())
            {
                for (var i = 0; i < buffers.Size; i++)
                {
                    bjro.GetPropertyByIndex(buffers.Properties[i], ref prop);
                    if (first == false)
                    {
                        writer.WriteComma();
                    }

                    first = false;
                    writer.WritePropertyName(prop.Name);
                    writer.WriteValue(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                }
            }
            writer.WriteEndObject();
        }
    }
}
