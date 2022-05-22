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
using Raven.Client.Extensions;
using Raven.Server.Documents.Handlers.Processors.Subscriptions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Sharding.Handlers.Processors.Subscriptions;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions.SubscriptionProcessor;
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

                SubscriptionProcessor processor;
                if(sub.Revisions)
                    processor = new TestRevisionsSubscriptionProcessor(Server.ServerStore, Database, state, sub.Collection, new SubscriptionWorkerOptions("dummy"), new IPEndPoint(HttpContext.Connection.RemoteIpAddress, HttpContext.Connection.RemotePort));
                else
                    processor = new TestDocumentsSubscriptionProcessor(Server.ServerStore, Database, state, sub.Collection, new SubscriptionWorkerOptions("dummy"), new IPEndPoint(HttpContext.Connection.RemoteIpAddress, HttpContext.Connection.RemotePort));
                processor.AddScript(patch);

                using (processor)
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                using (Database.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext clusterOperationContext))
                using (clusterOperationContext.OpenReadTransaction())
                using (context.OpenReadTransaction())
                {
                    processor.InitializeForNewBatch(clusterOperationContext, context, includeCmd);

                    writer.WriteStartObject();
                    writer.WritePropertyName("Results");
                    writer.WriteStartArray();
                    var numberOfDocs = 0;
                    while (numberOfDocs == 0 && sp.Elapsed < timeLimit)
                    {
                        var first = true;
                        var lastEtag = startEtag;
                        
                        ((IEtagSettable)processor).SetStartEtag(startEtag);

                        foreach (var itemDetails in processor.GetBatch())
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
                    includeCmd.Fill(includes, includeMissingAsNull: false);
                    await writer.WriteIncludesAsync(context, includes);
                    writer.WriteEndObject();
                }
            }
        }

        [RavenAction("/databases/*/subscriptions", "PUT", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Create()
        {
            using (var processor = new SubscriptionsHandlerProcessorForPutSubscription(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/subscriptions", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Delete()
        {
            using (var processor = new SubscriptionHandlerProcessorForDeleteSubscription(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/subscriptions/state", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetSubscriptionState()
        {
            using (var processor = new SubscriptionsHandlerProcessorForGetSubscriptionState(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/debug/subscriptions/resend", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetSubscriptionResend()
        {
            var subscriptionName = GetStringQueryString("name");

            using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var subscriptionState = Database
                    .SubscriptionStorage
                    .GetSubscriptionFromServerStore(subscriptionName);

                if (subscriptionState == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                var subscriptionConnections = Database.SubscriptionStorage.GetSubscriptionConnectionsState(context, subscriptionName);
                var items = SubscriptionStorage.GetResendItems(context, Database.Name, subscriptionState.SubscriptionId);

                writer.WriteStartObject();
                writer.WriteArray("Active", subscriptionConnections.GetActiveBatches());
                writer.WriteComma();
                writer.WriteArray("Results", items.Select(i => i.ToJson()), context);
                writer.WriteEndObject();
            }
        }

        [RavenAction("/databases/*/subscriptions/connection-details", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, CorsMode = CorsMode.Cluster)]
        public async Task GetSubscriptionConnectionDetails()
        {
            using (var processor = new SubscriptionsHandlerProcessorForGetConnectionDetails(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/subscriptions", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task GetAll()
        {
            using (var processor = new SubscriptionsHandlerProcessorForGetSubscription(this))
                await processor.ExecuteAsync();
        }
        
        [RavenAction("/databases/*/subscriptions/performance/live", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, SkipUsagesCount = true)]
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


        [RavenAction("/databases/*/subscriptions/drop", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task DropSubscriptionConnection()
        {
            using (var processor = new SubscriptionsHandlerProcessorForDropSubscriptionConnection(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/subscriptions/update", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Update()
        {
            using (var processor = new SubscriptionsHandlerProcessorForPostSubscription(this))
                await processor.ExecuteAsync();
        }

        public static bool SubscriptionHasChanges(SubscriptionCreationOptions options, SubscriptionState state)
        {
            bool gotChanges = options.Name != state.SubscriptionName
                              || options.ChangeVector != state.ChangeVectorForNextBatchStartingPoint
                              || options.MentorNode != state.MentorNode
                              || options.Query != state.Query;

            return gotChanges;
        }

        public async Task CreateInternalAsync(BlittableJsonReaderObject bjro, SubscriptionCreationOptions options, DocumentsOperationContext context, long? id, bool? disabled)
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
            (long index, long subscriptionId) = await Database.SubscriptionStorage.PutSubscription(options, GetRaftRequestIdFromQuery(), id, disabled, mentor);

            var name = options.Name ?? subscriptionId.ToString();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
            using (serverContext.OpenReadTransaction())
            {
                // need to wait on the relevant remote node
                var node = Database.SubscriptionStorage.GetResponsibleNode(serverContext, name);
                if (node != null && node != ServerStore.NodeTag)
                {
                    await WaitForExecutionOnSpecificNode(serverContext, ServerStore.GetClusterTopology(serverContext), node, index);
                }
            }

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
