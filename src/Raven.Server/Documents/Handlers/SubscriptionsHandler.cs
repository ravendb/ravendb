using System;
using System.Net;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Handlers.Processors.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public sealed class SubscriptionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/subscriptions/try", "POST", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task Try()
        {
            using (var processor = new SubscriptionsHandlerProcessorForTrySubscription(this))
                await processor.ExecuteAsync();
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
            using (var processor = new SubscriptionsHandlerProcessorForGetResend(this))
                await processor.ExecuteAsync();
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
            using (var processor = new SubscriptionsHandlerProcessorForPerformanceLive(this))
                await processor.ExecuteAsync();
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
                              || options.Query != state.Query
                              || options.PinToMentorNode != state.PinToMentorNode
                              || options.Disabled != state.Disabled
                              || options.ArchivedDataProcessingBehavior != state.ArchivedDataProcessingBehavior;

            return gotChanges;
        }

        public async Task CreateInternalAsync(BlittableJsonReaderObject bjro, SubscriptionCreationOptions options, DocumentsOperationContext context, long? id, bool? disabled, SubscriptionConnection.ParsedSubscription sub)
        {
            if (TrafficWatchManager.HasRegisteredClients)
                AddStringToHttpContext(bjro.ToString(), TrafficWatchChangeType.Subscriptions);

            if (Enum.TryParse(options.ChangeVector, out Constants.Documents.SubscriptionChangeVectorSpecialStates changeVectorSpecialValue))
            {
                switch (changeVectorSpecialValue)
                {
                    case Constants.Documents.SubscriptionChangeVectorSpecialStates.BeginningOfTime:

                        options.ChangeVector = null;
                        break;

                    case Constants.Documents.SubscriptionChangeVectorSpecialStates.LastDocument:
                        options.ChangeVector = Database.SubscriptionStorage.GetLastDocumentChangeVectorForSubscription(context, sub.Collection);
                        break;
                }
            }

            var mentor = options.MentorNode;
            (long index, long subscriptionId) = await Database.SubscriptionStorage.PutSubscription(options, GetRaftRequestIdFromQuery(), id, disabled, mentor);

            var name = options.Name ?? subscriptionId.ToString();

            using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext serverContext))
            using (serverContext.OpenReadTransaction())
            {
                // need to wait on the relevant remote node
                var node = Database.SubscriptionStorage.GetResponsibleNode(serverContext, name);
                if (node != null && node != ServerStore.NodeTag)
                {
                    await ServerStore.WaitForExecutionOnSpecificNodeAsync(serverContext, node, subscriptionId);
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

    public sealed class DocumentWithException : IDynamicJson
    {
        public string Id { get; set; }
        public string ChangeVector { get; set; }
        public string Exception { get; set; }
        public object DocumentData { get; set; }

        public DynamicJsonValue ToJson()
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
