using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions.SubscriptionProcessor;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions
{
    internal class SubscriptionsHandlerProcessorForTrySubscription : AbstractSubscriptionsHandlerProcessorForTrySubscription<DatabaseRequestHandler, DocumentsOperationContext>
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
                            state.ChangeVectorForNextBatchStartingPoint = RequestHandler.Database.DocumentsStorage.GetLastDocumentChangeVector(context.Transaction.InnerTransaction, context, subscription.Collection);
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

            DatabaseSubscriptionProcessor processor;
            if (subscription.Revisions)
                processor = new TestRevisionsDatabaseSubscriptionProcessor(RequestHandler.Server.ServerStore, RequestHandler.Database, state, subscription, new SubscriptionWorkerOptions("dummy"), new IPEndPoint(HttpContext.Connection.RemoteIpAddress, HttpContext.Connection.RemotePort));
            else
                processor = new TestDocumentsDatabaseSubscriptionProcessor(RequestHandler.Server.ServerStore, RequestHandler.Database, state, subscription, new SubscriptionWorkerOptions("dummy"), new IPEndPoint(HttpContext.Connection.RemoteIpAddress, HttpContext.Connection.RemotePort));
            
            processor.Patch = patch;

            using (processor)
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            using (RequestHandler.Database.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext clusterOperationContext))
            using (clusterOperationContext.OpenReadTransaction())
            using (processor.InitializeForNewBatch(clusterOperationContext, out var includeCmd))
            {
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
                                includeCmd.IncludeDocumentsCommand?.Gather(itemDetails.Doc);

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
                includeCmd.IncludeDocumentsCommand?.Fill(includes, includeMissingAsNull: false);
                await writer.WriteIncludesAsync(context, includes);
                writer.WriteEndObject();
            }
        }
    }
}
