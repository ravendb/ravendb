using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Sharding;
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

                await CreateSubscriptionInternal(id, options, context);
            }
        }

        private async Task CreateSubscriptionInternal(long? id, SubscriptionCreationOptions options, TransactionOperationContext context)
        {
            var disposables = new List<IDisposable>();
            try
            {
                var cmds = new List<CreateSubscriptionCommand>();
                var tasks = new List<Task>();
                foreach (var re in ShardedContext.RequestExecutors)
                {
                    disposables.Add(ContextPool.AllocateOperationContext(out TransactionOperationContext ctx));
                    var cmd = new CreateSubscriptionCommand(conventions: null, options, id.HasValue ? $"{id}" : null);
                    cmds.Add(cmd);
                    var t = re.ExecuteAsync(cmd, ctx);
                    tasks.Add(t);
                    if (id.HasValue == false)
                    {
                        // wait on first command to have equal id for all subscriptions
                        await t;
                        id = cmd.Result.RaftIndex;
                    }
                }

                await Task.WhenAll(tasks);
                Debug.Assert(cmds.All(x => x.Result.Name == cmds.First().Result.Name));

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(CreateSubscriptionResult.Name)] = cmds.First().Result.Name
                    });
                }
            }
            finally
            {
                disposables.ForEach(x => x.Dispose());
            }
        }

        [RavenShardedAction("/databases/*/subscriptions", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Delete()
        {
            var subscriptionName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("taskName");
            var disposables = new List<IDisposable>();

            try
            {
                var tasks = new List<Task>();
                foreach (var re in ShardedContext.RequestExecutors)
                {
                    disposables.Add(ContextPool.AllocateOperationContext(out TransactionOperationContext ctx));
                    tasks.Add(re.ExecuteAsync(new DeleteSubscriptionCommand(subscriptionName), ctx));
                }

                await Task.WhenAll(tasks);
                await NoContent();
            }
            finally
            {
                disposables.ForEach(x => x.Dispose());

            }
        }

        //TODO: egor attribute
        [RavenShardedAction("/databases/*/subscriptions", "GET", AuthorizationStatus.ValidUser, EndpointType.Read/*, IsDebugInformationEndpoint = true*/)]
        public async Task GetAll()
        {
            var start = GetStart();
            var pageSize = GetPageSize();

            var disposables = new List<IDisposable>();
            try
            {
                var tasks = new List<Task>();
                var cmds = new List<GetSubscriptionsCommand>();
                foreach (var re in ShardedContext.RequestExecutors)
                {
                    disposables.Add(ContextPool.AllocateOperationContext(out TransactionOperationContext ctx));
                    var cmd = new GetSubscriptionsCommand(start, pageSize);
                    cmds.Add(cmd);
                    tasks.Add(re.ExecuteAsync(cmd, ctx));
                }
                await Task.WhenAll(tasks);
                Debug.Assert(cmds.All(x => x.Result.Length == cmds.First().Result.Length));

                HashSet<string> names = new HashSet<string>();
                HashSet<SubscriptionState> subscriptions = new HashSet<SubscriptionState>();

                foreach (var cmd in cmds)
                {
                    foreach (var subscription in cmd.Result)
                    {
                        if (names.Add(subscription.SubscriptionName))
                        {
                            subscriptions.Add(subscription);
                        }
                    }
                }
                using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteStartObject();
                        writer.WriteArray(context, "Results", SubscriptionsHandler.GetSubscriptionStateBlittable(subscriptions), (w, c, subscription) => c.Write(w, subscription));
                        writer.WriteEndObject();
                    }
                }
            }
            finally
            {
                disposables.ForEach(x => x.Dispose());
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
                        state = SubscriptionStorage.GetSubscriptionFromServerStore(ServerStore, context, ShardedContext.GetShardedDatabaseName(), options.Name);
                        id = state.SubscriptionId;
                    }
                    else
                    {
                        state = SubscriptionStorage.GetSubscriptionFromServerStoreById(ServerStore, ShardedContext.GetShardedDatabaseName(), id.Value);

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
                            await CreateSubscriptionInternal(id: null, options, context);
                            return;
                        }

                        if (options.Name == null)
                        {
                            // subscription with such id doesn't exist, add new subscription using id
                            await CreateSubscriptionInternal(id, options, context);
                            return;
                        }

                        // this is the case when we have both name and id, and there no subscription with such id
                        try
                        {
                            // check the name
                            state = SubscriptionStorage.GetSubscriptionFromServerStore(ServerStore, context, ShardedContext.GetShardedDatabaseName(), options.Name);
                            id = state.SubscriptionId;
                        }
                        catch (SubscriptionDoesNotExistException)
                        {
                            // subscription with such id or name doesn't exist, add new subscription using both name and id
                            await CreateSubscriptionInternal(id, options, context);
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

                await CreateSubscriptionInternal(id, options, context);
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
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
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

                context.Write(writer, subscriptionConnectionDetails.ToJson());
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

    public class SubscriptionTryoutCommand : RavenCommand<GetDocumentsResult>
    {
        private readonly SubscriptionTryout _tryout;
        private readonly int _pageSize;

        public SubscriptionTryoutCommand(SubscriptionTryout tryout, int pageSize)
        {
            _tryout = tryout;
            _pageSize = pageSize;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/subscriptions/try?pageSize={_pageSize}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                    {
                        writer.WriteStartObject();
                        writer.WritePropertyName(nameof(SubscriptionTryout.ChangeVector));
                        writer.WriteString(_tryout.ChangeVector);
                        writer.WritePropertyName(nameof(SubscriptionTryout.Query));
                        writer.WriteString(_tryout.Query);
                        writer.WriteEndObject();
                    }
                })
            };

            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
            {
                Result = null;
                return;
            }

            Result = JsonDeserializationClient.GetDocumentsResult(response);
        }

        public override bool IsReadRequest => true;
    }
}
