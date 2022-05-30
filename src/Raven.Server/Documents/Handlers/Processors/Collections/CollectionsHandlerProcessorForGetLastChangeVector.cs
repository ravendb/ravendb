using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Collections
{
    internal class CollectionsHandlerProcessorForGetLastChangeVector : AbstractCollectionsHandlerProcessorForGetLastChangeVector<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public CollectionsHandlerProcessorForGetLastChangeVector([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override bool SupportsCurrentNode => true;

        protected override async ValueTask HandleCurrentNodeAsync()
        {
            var collection = GetCollectionName();
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var result = RequestHandler.Database.DocumentsStorage.GetLastDocumentChangeVector(context.Transaction.InnerTransaction, context, collection);
                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName(nameof(LastChangeVectorForCollectionResult.Collection));
                    writer.WriteString(collection);
                    writer.WritePropertyName(nameof(LastChangeVectorForCollectionResult.LastChangeVector));
                    writer.WriteString(result);
                    writer.WriteEndObject();
                }
            }
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<LastChangeVectorForCollectionResult> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
    }
}
