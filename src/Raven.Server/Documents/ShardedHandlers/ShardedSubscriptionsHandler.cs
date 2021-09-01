using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ShardedHandlers
{
    public class ShardedSubscriptionsHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/subscriptions", "PUT", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Create()
        {
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), null);
                if (TrafficWatchManager.HasRegisteredClients)
                    AddStringToHttpContext(json.ToString(), TrafficWatchChangeType.Subscriptions);

                var options = JsonDeserializationServer.SubscriptionCreationParams(json);
                var id = GetLongQueryString("id", required: false);
                bool? disabled = GetBoolValueQueryString("disabled", required: false);

                await CreateSubscriptionInternal(id, disabled, options, context);
            }
        }

        [RavenShardedAction("/databases/*/subscriptions/update", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Update()
        {
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), null);
                var options = JsonDeserializationServer.SubscriptionUpdateOptions(json);
                var id = options.Id;

                SubscriptionState state;

                try
                {
                    if (id == null)
                    {
                        state = SubscriptionStorageBase.GetSubscriptionFromServerStore(ServerStore, context, ShardedContext.DatabaseName, options.Name);
                        id = state.SubscriptionId;
                    }
                    else
                    {
                        state = SubscriptionStorageBase.GetSubscriptionFromServerStoreById(ServerStore, ShardedContext.DatabaseName, id.Value);

                        // keep the old subscription name
                        if (options.Name == null)
                            options.Name = state.SubscriptionName;
                    }
                }
                catch (SubscriptionDoesNotExistException)
                {
                    if (options.CreateNew)
                    {
                        if (id == null)
                        {
                            // subscription with such name doesn't exist, add new subscription
                            await CreateSubscriptionInternal(id: null, disabled: false, options, context);
                            return;
                        }

                        if (options.Name == null)
                        {
                            // subscription with such id doesn't exist, add new subscription using id
                            await CreateSubscriptionInternal(id, disabled: false, options, context);
                            return;
                        }

                        // this is the case when we have both name and id, and there no subscription with such id
                        try
                        {
                            // check the name
                            state = SubscriptionStorageBase.GetSubscriptionFromServerStore(ServerStore, context, ShardedContext.DatabaseName, options.Name);
                            id = state.SubscriptionId;
                        }
                        catch (SubscriptionDoesNotExistException)
                        {
                            // subscription with such id or name doesn't exist, add new subscription using both name and id
                            await CreateSubscriptionInternal(id, disabled: false, options, context);
                            return;
                        }
                    }
                    else
                    {
                        throw;
                    }
                }

                if (options.ChangeVector == null)
                    options.ChangeVector = state.ChangeVectorForNextBatchStartingPoint;

                if (options.MentorNode == null)
                    options.MentorNode = state.MentorNode;

                if (options.Query == null)
                    options.Query = state.Query;

                if (SubscriptionsHandler.SubscriptionHasChanges(options, state) == false)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return;
                }

                await CreateSubscriptionInternal(id, disabled: false, options, context);
            }
        }

        private async Task CreateSubscriptionInternal(long? id, bool? disabled, SubscriptionCreationOptions options, JsonOperationContext context)
        {
            var sub = SubscriptionConnectionBase.ParseSubscriptionQuery(options.Query);
            if (Enum.TryParse(options.ChangeVector, out Client.Constants.Documents.SubscriptionChangeVectorSpecialStates changeVectorSpecialValue))
            {
                switch (changeVectorSpecialValue)
                {
                    case Client.Constants.Documents.SubscriptionChangeVectorSpecialStates.BeginningOfTime:
                        options.ChangeVector = null;
                        break;

                    case Client.Constants.Documents.SubscriptionChangeVectorSpecialStates.LastDocument:
                        options.ChangeVector = await ShardedContext.GetLastDocumentChangeVectorForCollection(sub.Collection);
                        break;
                }
            }

            var etag = await ShardedContext.ShardedSubscriptionStorage.PutSubscription(ShardedContext.DatabaseName, options, GetRaftRequestIdFromQuery(), id, disabled, options.MentorNode);

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

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
            using (serverContext.OpenReadTransaction())
            {
                var responsibleNodes = ShardedContext.ShardedSubscriptionStorage.GetResponsibleNodes(serverContext, name);
                foreach (var node in responsibleNodes)
                {
                    if (node != ServerStore.NodeTag)
                    {
                        await WaitForExecutionOnSpecificNode(serverContext, ServerStore.GetClusterTopology(serverContext), node, index);

                    }
                }
            }

            HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    [nameof(CreateSubscriptionResult.Name)] = name,
                    [nameof(CreateSubscriptionResult.RaftIndex)] = index
                });
            }
        }

        [RavenShardedAction("/databases/*/subscriptions", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Delete()
        {
            var subscriptionName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("taskName");
            await ShardedContext.ShardedSubscriptionStorage.DeleteSubscription(ShardedContext.DatabaseName, subscriptionName, GetRaftRequestIdFromQuery());
            await NoContent();
        }

        //TODO: egor attribute
        [RavenShardedAction("/databases/*/subscriptions", "GET", AuthorizationStatus.ValidUser, EndpointType.Read /*, IsDebugInformationEndpoint = true*/)]
        public async Task GetAll()
        {
            var start = GetStart();
            var pageSize = GetPageSize();
            var history = GetBoolValueQueryString("history", required: false) ?? false;
            var running = GetBoolValueQueryString("running", required: false) ?? false;
            var id = GetLongQueryString("id", required: false);
            var name = GetStringQueryString("name", required: false);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                if (SubscriptionsHandler.GetAllInternal(ShardedContext.ShardedSubscriptionStorage, context, ShardedContext.DatabaseName, name, id, running, history, start, pageSize, out IEnumerable<SubscriptionGeneralDataAndStats> subscriptions))
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    SubscriptionsHandler.WriteGetAllResult(writer, subscriptions, context);
                }
            }
        }

        [RavenShardedAction("/databases/*/subscriptions/drop", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task DropSubscriptionConnection()
        {
            var subscriptionId = GetLongQueryString("id", required: false);
            var subscriptionName = GetStringQueryString("name", required: false);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var subscription = ShardedContext.ShardedSubscriptionStorage.GetRunningSubscription(context, subscriptionId, subscriptionName, history: false);
                if (subscription == null)
                    return;

                if (await ShardedContext.ShardedSubscriptionStorage.DropSubscriptionConnection(subscription.SubscriptionId, new SubscriptionClosedException("Dropped by API request")) == false)
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            }
        }

        //TODO: egor attribute
        [RavenShardedAction("/databases/*/subscriptions/connection-details", "GET", AuthorizationStatus.ValidUser, EndpointType.Read/*, CorsMode = CorsMode.Cluster*/)]
        public async Task GetSubscriptionConnectionDetails()
        {
            var subscriptionName = GetStringQueryString("name", false);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                if (string.IsNullOrEmpty(subscriptionName))
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                    return;
                }

                var state = ShardedContext.ShardedSubscriptionStorage.GetSubscriptionConnection(context, subscriptionName);

                var subscriptionConnectionDetails = new SubscriptionConnectionDetails
                {
                    ClientUri = state?.Connection?.ClientUri,
                    Strategy = state?.Connection?.Strategy
                };

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, subscriptionConnectionDetails.ToJson());
                }
            }
        }

        //TODO: egor attribute
        [RavenShardedAction("/databases/*/subscriptions/try", "POST", AuthorizationStatus.ValidUser, EndpointType.Write/*, DisableOnCpuCreditsExhaustion = true*/)]
        public async Task Try()
        {
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                using var json = await context.ReadForMemoryAsync(RequestBodyStream(), null);
                var tryout = JsonDeserializationServer.SubscriptionTryout(json);
                var sub = SubscriptionConnectionBase.ParseSubscriptionQuery(tryout.Query);
                if (sub.Collection == null)
                    throw new ArgumentException("Collection must be specified");

                const int maxPageSize = 1024;
                var pageSize = GetIntValueQueryString("pageSize") ?? 1;
                if (pageSize > maxPageSize)
                    throw new ArgumentException($"Cannot gather more than {maxPageSize} results during tryouts, but requested number was {pageSize}.");

                var disposables = new List<IDisposable>();
                try
                {
                    var cmds = new List<SubscriptionTryoutCommand>();
                    var tasks = new List<Task>();
                    foreach (var re in ShardedContext.RequestExecutors)
                    {
                        disposables.Add(ContextPool.AllocateOperationContext(out TransactionOperationContext ctx));
                        var cmd = new SubscriptionTryoutCommand(tryout, pageSize);
                        cmds.Add(cmd);
                        var t = re.ExecuteAsync(cmd, ctx);
                        tasks.Add(t);
                    }
                    //TODO: egor RavenDB-16279 handle includes from other shards
                    await Task.WhenAll(tasks);

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    using (context.OpenReadTransaction())
                    {
                        writer.WriteStartObject();
                        writer.WritePropertyName("Results");
                        writer.WriteStartArray();
                        var numberOfDocs = 0;
                        string lastId = null;
                        var f = true;
                        foreach (var cmd in cmds)
                        {
                            foreach (var res in cmd.Result.Results)
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
                                    if (bjro.TryGetMember(nameof(Client.Constants.Documents.Metadata.Key), out var obj) && obj is BlittableJsonReaderObject metadata
                                        && metadata.TryGetMember(nameof(Client.Constants.Documents.Metadata.Id), out var obj2) && obj2 is string id)
                                    {
                                        lastId = id;
                                    }

                                    numberOfDocs++;
                                }
                            }
                            if (numberOfDocs == pageSize)
                                break;
                        }

                        writer.WriteEndArray();
                        writer.WriteComma();
                        writer.WritePropertyName("Includes");
                        //TODO: egor RavenDB-16279
                        // write all includes until lastId
                        writer.WriteStartObject();
                        writer.WriteEndObject();
                        writer.WriteEndObject();
                    }
                }
                finally
                {
                    disposables.ForEach(x => x.Dispose());
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
