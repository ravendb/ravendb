using System;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Commands.Revisions;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Revisions
{
    internal class RevisionsHandlerProcessorForGetResolvedRevisions : AbstractRevisionsHandlerProcessorForGetResolvedRevisions<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RevisionsHandlerProcessorForGetResolvedRevisions([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask GetResolvedRevisionsAndWriteAsync(DocumentsOperationContext context, DateTime since, int take, CancellationToken token)
        {
            using (context.OpenReadTransaction())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                var revisions = RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetResolvedDocumentsSince(context, since, take);

                writer.WriteStartObject();
                writer.WritePropertyName(nameof(ResolvedRevisions.Results));
                await writer.WriteDocumentsAsync(context, revisions, metadataOnly: false, token);
                writer.WriteEndObject();
            }
        }
    }
}
