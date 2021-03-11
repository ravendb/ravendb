using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class SubscriptionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/subscriptions/try", "POST", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task Try()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                BlittableJsonReaderObject json = await context.ReadForMemoryAsync(RequestBodyStream(), null);
                SubscriptionTryout tryout = JsonDeserializationServer.SubscriptionTryout(json);

                SubscriptionConnection.ParsedSubscription sub = SubscriptionConnection.ParseSubscriptionQuery(tryout.Query);
                SubscriptionPatchDocument patch = null;
                if (string.IsNullOrEmpty(sub.Script) == false)
                {
                    patch = new SubscriptionPatchDocument(sub.Script, sub.Functions);
                }

                if (sub.Collection == null)
                    throw new ArgumentException("Collection must be specified");

                const int maxPageSize = 1024;
                var pageSize = GetIntValueQueryString("pageSize") ?? 1;
                if (pageSize > maxPageSize)
                    throw new ArgumentException($"Cannot gather more than {maxPageSize} results during tryouts, but requested number was {pageSize}.");

                var state = new SubscriptionState
                {
                    ChangeVectorForNextBatchStartingPoint = tryout.ChangeVector,
                    Query = tryout.Query
                };

                var fetcher = new SubscriptionDocumentsFetcher(Database, int.MaxValue, -0x42,
                    new IPEndPoint(HttpContext.Connection.RemoteIpAddress, HttpContext.Connection.RemotePort), sub.Collection, sub.Revisions, state, patch);

                var includeCmd = new IncludeDocumentsCommand(Database.DocumentsStorage, context, sub.Includes, isProjection: patch != null);

                if (Enum.TryParse(
                    tryout.ChangeVector,
                    out Constants.Documents.SubscriptionChangeVectorSpecialStates changeVectorSpecialValue))
                {
                    switch (changeVectorSpecialValue)
                    {
                        case Constants.Documents.SubscriptionChangeVectorSpecialStates.BeginningOfTime:
                        case Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange:
                            state.ChangeVectorForNextBatchStartingPoint = null;
                            break;

                        case Constants.Documents.SubscriptionChangeVectorSpecialStates.LastDocument:
                            using (context.OpenReadTransaction())
                            {
                                state.ChangeVectorForNextBatchStartingPoint = Database.DocumentsStorage.GetLastDocumentChangeVector(context.Transaction.InnerTransaction, context, sub.Collection);
                            }
                            break;
                    }
                }
                else
                {
                    state.ChangeVectorForNextBatchStartingPoint = tryout.ChangeVector;
                }

                var changeVector = state.ChangeVectorForNextBatchStartingPoint.ToChangeVector();
                var cv = changeVector.FirstOrDefault(x => x.DbId == Database.DbBase64Id);

                var sp = Stopwatch.StartNew();
                var timeLimit = TimeSpan.FromSeconds(GetIntValueQueryString("timeLimit", false) ?? 15);
                var startEtag = cv.Etag;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                using (context.OpenReadTransaction())
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Results");
                    writer.WriteStartArray();
                    var numberOfDocs = 0;
                    while (numberOfDocs == 0 && sp.Elapsed < timeLimit)
                    {
                        var first = true;
                        var lastEtag = startEtag;
                        foreach (var itemDetails in fetcher.GetDataToSend(context, includeCmd, startEtag))
                        {
                            if (itemDetails.Doc.Data != null)
                            {
                                using (itemDetails.Doc.Data)
                                {
                                    includeCmd.Gather(itemDetails.Doc);

                                    if (first == false)
                                        writer.WriteComma();

                                    if (itemDetails.Exception == null)
                                    {
                                        writer.WriteDocument(context, itemDetails.Doc, metadataOnly: false);
                                    }
                                    else
                                    {
                                        var documentWithException = new DocumentWithException
                                        {
                                            Exception = itemDetails.Exception.ToString(),
                                            ChangeVector = itemDetails.Doc.ChangeVector,
                                            Id = itemDetails.Doc.Id,
                                            DocumentData = itemDetails.Doc.Data
                                        };
                                        writer.WriteObject(context.ReadObject(documentWithException.ToJson(), ""));
                                    }

                                    first = false;

                                    if (++numberOfDocs >= pageSize)
                                        break;
                                }
                            }

                            if (sp.Elapsed >= timeLimit)
                                break;

                            lastEtag = itemDetails.Doc.Etag;
                        }

                        if (startEtag == lastEtag)
                            break;

                        startEtag = lastEtag;
                    }

                    writer.WriteEndArray();
                    writer.WriteComma();
                    writer.WritePropertyName("Includes");
                    var includes = new List<Document>();
                    includeCmd.Fill(includes);
                    await writer.WriteIncludesAsync(context, includes);
                    writer.WriteEndObject();
                }
            }
        }

        [RavenAction("/databases/*/subscriptions", "PUT", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Create()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), null);
                var options = JsonDeserializationServer.SubscriptionCreationParams(json);
                var id = GetLongQueryString("id", required: false);
                var disabled = GetBoolValueQueryString("disabled", required: false);

                await CreateInternal(json, options, context, id, disabled);
            }
        }

        [RavenAction("/databases/*/subscriptions", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Delete()
        {
            var subscriptionName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("taskName");

            await Database.SubscriptionStorage.DeleteSubscription(subscriptionName, GetRaftRequestIdFromQuery());
            
            Database.SubscriptionStorage.RaiseNotificationForTaskRemoved(subscriptionName);

            await NoContent();
        }

        [RavenAction("/databases/*/subscriptions/state", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetSubscriptionState()
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

                var subscriptionState = Database
                    .SubscriptionStorage
                    .GetSubscriptionFromServerStore(subscriptionName);

                if (subscriptionState == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                context.Write(writer, subscriptionState.ToJson());
            }
        }

        [RavenAction("/databases/*/subscriptions/connection-details", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, CorsMode = CorsMode.Cluster)]
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

                var state = Database.SubscriptionStorage.GetSubscriptionConnection(context, subscriptionName);

                var subscriptionConnectionDetails = new SubscriptionConnectionDetails
                {
                    ClientUri = state?.Connection?.ClientUri,
                    Strategy = state?.Connection?.Strategy
                };

                context.Write(writer, subscriptionConnectionDetails.ToJson());
                return;
            }
        }

        [RavenAction("/databases/*/subscriptions", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
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
                IEnumerable<SubscriptionStorage.SubscriptionGeneralDataAndStats> subscriptions;
                if (string.IsNullOrEmpty(name) && id == null)
                {
                    subscriptions = running
                        ? Database.SubscriptionStorage.GetAllRunningSubscriptions(context, history, start, pageSize)
                        : Database.SubscriptionStorage.GetAllSubscriptions(context, history, start, pageSize);
                }
                else
                {
                    var subscription = running
                        ? Database
                            .SubscriptionStorage
                            .GetRunningSubscription(context, id, name, history)
                        : Database
                            .SubscriptionStorage
                            .GetSubscription(context, id, name, history);

                    if (subscription == null)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return;
                    }

                    subscriptions = new[] { subscription };
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    var subscriptionsAsBlittable = subscriptions.Select(x => new DynamicJsonValue()
                    {
                        [nameof(SubscriptionState.SubscriptionId)] = x.SubscriptionId,
                        [nameof(SubscriptionState.SubscriptionName)] = x.SubscriptionName,
                        [nameof(SubscriptionState.ChangeVectorForNextBatchStartingPoint)] = x.ChangeVectorForNextBatchStartingPoint,
                        [nameof(SubscriptionState.Query)] = x.Query,
                        [nameof(SubscriptionState.Disabled)] = x.Disabled,
                        [nameof(SubscriptionState.LastClientConnectionTime)] = x.LastClientConnectionTime,
                        [nameof(SubscriptionState.LastBatchAckTime)] = x.LastBatchAckTime,
                        ["Connection"] = GetSubscriptionConnectionJson(x.Connection),
                        ["RecentConnections"] = x.RecentConnections?.Select(r => new DynamicJsonValue()
                        {
                            ["State"] = new DynamicJsonValue()
                            {
                                ["LatestChangeVectorClientACKnowledged"] = r.SubscriptionState.ChangeVectorForNextBatchStartingPoint,
                                ["Query"] = r.SubscriptionState.Query
                            },
                            ["Connection"] = GetSubscriptionConnectionJson(r)
                        }),
                        ["FailedConnections"] = x.RecentRejectedConnections?.Select(r => new DynamicJsonValue()
                        {
                            ["State"] = new DynamicJsonValue()
                            {
                                ["LatestChangeVectorClientACKnowledged"] = r.SubscriptionState.ChangeVectorForNextBatchStartingPoint,
                                ["Query"] = r.SubscriptionState.Query
                            },
                            ["Connection"] = GetSubscriptionConnectionJson(r)
                        }).ToList()
                    });

                    writer.WriteArray(context, "Results", subscriptionsAsBlittable, (w, c, subscription) => c.Write(w, subscription));

                    writer.WriteEndObject();
                }
            }
        }
        
        [RavenAction("/databases/*/subscriptions/performance/live", "GET", AuthorizationStatus.ValidUser, SkipUsagesCount = true)]
        public async Task PerformanceLive()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                var receiveBuffer = new ArraySegment<byte>(new byte[1024]);
                var receive = webSocket.ReceiveAsync(receiveBuffer, Database.DatabaseShutdown);

                using (var ms = new MemoryStream())
                using (var collector = new LiveSubscriptionPerformanceCollector(Database))
                {
                    // 1. Send data to webSocket without making UI wait upon opening webSocket
                    await collector.SendStatsOrHeartbeatToWebSocket(receive, webSocket, ContextPool, ms, 100);

                    // 2. Send data to webSocket when available
                    while (Database.DatabaseShutdown.IsCancellationRequested == false)
                    {
                        if (await collector.SendStatsOrHeartbeatToWebSocket(receive, webSocket, ContextPool, ms, 4000) == false)
                        {
                            break;
                        }
                    }
                }
            }
        }

        private static DynamicJsonValue GetSubscriptionConnectionJson(SubscriptionConnection x)
        {
            if (x == null)
                return new DynamicJsonValue();

            return new DynamicJsonValue()
            {
                [nameof(SubscriptionConnection.ClientUri)] = x.ClientUri,
                [nameof(SubscriptionConnection.Strategy)] = x.Strategy,
                [nameof(SubscriptionConnection.Stats)] = GetConnectionStatsJson(x.Stats),
                [nameof(SubscriptionConnection.ConnectionException)] = x.ConnectionException?.Message,
                ["TcpConnectionStats"] = x.TcpConnection.GetConnectionStats(),
                [nameof(SubscriptionConnection.RecentSubscriptionStatuses)] = new DynamicJsonArray(x.RecentSubscriptionStatuses?.ToArray() ?? Array.Empty<string>())
            };
        }

        private static DynamicJsonValue GetConnectionStatsJson(SubscriptionConnectionStats x)
        {
            return new DynamicJsonValue()
            {
                [nameof(SubscriptionConnectionStats.AckRate)] = x.AckRate?.CreateMeterData(),
                [nameof(SubscriptionConnectionStats.BytesRate)] = x.BytesRate?.CreateMeterData(),
                [nameof(SubscriptionConnectionStats.ConnectedAt)] = x.ConnectedAt,
                [nameof(SubscriptionConnectionStats.DocsRate)] = x.DocsRate?.CreateMeterData(),
                [nameof(SubscriptionConnectionStats.LastAckReceivedAt)] = x.LastAckReceivedAt,
                [nameof(SubscriptionConnectionStats.LastMessageSentAt)] = x.LastMessageSentAt,
            };
        }

        [RavenAction("/databases/*/subscriptions/drop", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public Task DropSubscriptionConnection()
        {
            var subscriptionId = GetLongQueryString("id", required: false);
            var subscriptionName = GetStringQueryString("name", required: false);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var subscription = Database
                    .SubscriptionStorage
                    .GetRunningSubscription(context, subscriptionId, subscriptionName, false);

                if (subscription != null)
                {
                    if (Database.SubscriptionStorage.DropSubscriptionConnection(subscription.SubscriptionId,
                            new SubscriptionClosedException("Dropped by API request")) == false)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return Task.CompletedTask;
                    }
                }
            }

            return NoContent();
        }

        [RavenAction("/databases/*/subscriptions/update", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Update()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
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
                        state = Database.SubscriptionStorage.GetSubscriptionFromServerStore(options.Name);
                        id = state.SubscriptionId;
                    }
                    else
                    {
                        state = Database.SubscriptionStorage.GetSubscriptionFromServerStoreById(id.Value);

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
                            await CreateInternal(json, options, context, id: null, disabled: false);
                            return;
                        }

                        if (options.Name == null)
                        {
                            // subscription with such id doesn't exist, add new subscription using id
                            await CreateInternal(json, options, context, id, disabled: false);
                            return;
                        }

                        // this is the case when we have both name and id, and there no subscription with such id
                        try
                        {
                            // check the name
                            state = Database.SubscriptionStorage.GetSubscriptionFromServerStore(options.Name);
                            id = state.SubscriptionId;
                        }
                        catch (SubscriptionDoesNotExistException)
                        {
                            // subscription with such id or name doesn't exist, add new subscription using both name and id
                            await CreateInternal(json, options, context, id, disabled: false);
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

                if (SubscriptionHasChanges(options, state) == false)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return;
                }

                await CreateInternal(json, options, context, id, disabled: false);
            }
        }

        private static bool SubscriptionHasChanges(SubscriptionCreationOptions options, SubscriptionState state)
        {
            bool gotChanges = options.Name != state.SubscriptionName
                              || options.ChangeVector != state.ChangeVectorForNextBatchStartingPoint
                              || options.MentorNode != state.MentorNode
                              || options.Query != state.Query;

            return gotChanges;
        }

        private async Task CreateInternal(BlittableJsonReaderObject bjro, SubscriptionCreationOptions options, DocumentsOperationContext context, long? id, bool? disabled)
        {
            if (TrafficWatchManager.HasRegisteredClients)
                AddStringToHttpContext(bjro.ToString(), TrafficWatchChangeType.Subscriptions);

            var sub = SubscriptionConnection.ParseSubscriptionQuery(options.Query);

            if (Enum.TryParse(options.ChangeVector, out Constants.Documents.SubscriptionChangeVectorSpecialStates changeVectorSpecialValue))
            {
                switch (changeVectorSpecialValue)
                {
                    case Constants.Documents.SubscriptionChangeVectorSpecialStates.BeginningOfTime:

                        options.ChangeVector = null;
                        break;

                    case Constants.Documents.SubscriptionChangeVectorSpecialStates.LastDocument:
                        options.ChangeVector = Database.DocumentsStorage.GetLastDocumentChangeVector(context.Transaction.InnerTransaction, context, sub.Collection);
                        break;
                }
            }

            var mentor = options.MentorNode;
            var subscriptionId = await Database.SubscriptionStorage.PutSubscription(options, GetRaftRequestIdFromQuery(), id, disabled, mentor);

            var name = options.Name ?? subscriptionId.ToString();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
            using (serverContext.OpenReadTransaction())
            {
                // need to wait on the relevant remote node
                var node = Database.SubscriptionStorage.GetResponsibleNode(serverContext, name);
                if (node != null && node != ServerStore.NodeTag)
                {
                    await WaitForExecutionOnSpecificNode(serverContext, ServerStore.GetClusterTopology(serverContext), node, subscriptionId);
                }
            }

            HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    [nameof(CreateSubscriptionResult.Name)] = name
                });
            }
        }
    }

    public class DocumentWithException : IDynamicJson
    {
        public string Id { get; set; }
        public string ChangeVector { get; set; }
        public string Exception { get; set; }
        public object DocumentData { get; set; }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(ChangeVector)] = ChangeVector,
                [nameof(Exception)] = Exception,
                [nameof(DocumentData)] = DocumentData
            };
        }
    }
}
