using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions.Processor;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal sealed class SubscriptionsHandlerProcessorForTrySubscription : AbstractSubscriptionsHandlerProcessorForTrySubscription<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public SubscriptionsHandlerProcessorForTrySubscription([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask TryoutSubscriptionAsync(DocumentsOperationContext context, SubscriptionConnection.ParsedSubscription subscription, SubscriptionTryout tryout, int pageSize)
        {
            SubscriptionPatchDocument patch = null;
            if (string.IsNullOrEmpty(subscription.Script) == false)
            {
                patch = new SubscriptionPatchDocument(subscription.Script, subscription.Functions);
            }
            var state = new SubscriptionState
            {
                ChangeVectorForNextBatchStartingPoint = tryout.ChangeVector,
                Query = tryout.Query
            };

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
                            state.ChangeVectorForNextBatchStartingPoint = RequestHandler.Database.SubscriptionStorage.GetLastDocumentChangeVectorForSubscription(context, subscription.Collection);
                        }
                        break;
                }
            }
            else
            {
                state.ChangeVectorForNextBatchStartingPoint = tryout.ChangeVector;
            }

            var changeVector = state.ChangeVectorForNextBatchStartingPoint.ToChangeVector();
            var cv = changeVector.FirstOrDefault(x => x.DbId == RequestHandler.Database.DbBase64Id);

            var sp = Stopwatch.StartNew();
            var timeLimit = TimeSpan.FromSeconds(RequestHandler.GetIntValueQueryString("timeLimit", false) ?? 15);
            var startEtag = cv.Etag;

            ISubscriptionProcessor<DatabaseIncludesCommandImpl> processor;
            if (subscription.Revisions)
                processor = new TestRevisionsDatabaseSubscriptionProcessor(RequestHandler.Server.ServerStore, RequestHandler.Database, state, subscription, new SubscriptionWorkerOptions("dummy"), new IPEndPoint(HttpContext.Connection.RemoteIpAddress, HttpContext.Connection.RemotePort), timeLimit, pageSize);
            else
                processor = new TestDocumentsDatabaseSubscriptionProcessor(RequestHandler.Server.ServerStore, RequestHandler.Database, state, subscription, new SubscriptionWorkerOptions("dummy"), new IPEndPoint(HttpContext.Connection.RemoteIpAddress, HttpContext.Connection.RemotePort), timeLimit, pageSize);

            ((IDatabaseSubscriptionProcessor)processor).Patch = patch;

            using (processor)
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            using (RequestHandler.Database.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext clusterOperationContext))
            using (clusterOperationContext.OpenReadTransaction())
            using (processor.InitializeForNewBatch(clusterOperationContext, out DatabaseIncludesCommandImpl includeDocuments))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Results");
                writer.WriteStartArray();

                var first = true;
                ((IEtagSettable)processor).SetStartEtag(startEtag);
                var items = await processor.GetBatchAsync(batchScope: null, sendingCurrentBatchStopwatch: sp);
                foreach (var itemDetails in items.CurrentBatch)
                {
                    if (itemDetails.Document.Data != null)
                    {
                        using (itemDetails.Document.Data)
                        {
                            includeDocuments?.GatherIncludesForDocument(itemDetails.Document);

                            if (first == false)
                                writer.WriteComma();

                            if (itemDetails.Exception == null)
                            {
                                writer.WriteDocument(context, itemDetails.Document, metadataOnly: false);
                            }
                            else
                            {
                                var documentWithException = new DocumentWithException
                                {
                                    Exception = itemDetails.Exception.ToString(),
                                    ChangeVector = itemDetails.Document.ChangeVector,
                                    Id = itemDetails.Document.Id,
                                    DocumentData = itemDetails.Document.Data
                                };

                                using (var documentWithExceptionReader = context.ReadObject(documentWithException.ToJson(), "TrySubscription"))
                                    writer.WriteObject(documentWithExceptionReader);
                            }

                            first = false;
                        }
                    }
                }

                writer.WriteEndArray();
               
                if (includeDocuments != null)
                {
                    writer.WriteComma();
                    writer.WritePropertyName("Includes");
                    await includeDocuments.WriteIncludesAsync(writer, context, batchScope: null, CancellationToken.None);
                }

                writer.WriteEndObject();
            }
        }
    }
}
