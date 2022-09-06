using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal class ReplicationHandlerProcessorForGetConflicts : AbstractReplicationHandlerProcessorForGetConflicts<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public ReplicationHandlerProcessorForGetConflicts([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override ValueTask<GetConflictsPreviewResult> GetConflictsPreviewAsync(DocumentsOperationContext context, long start, int pageSize)
        {
            using (context.OpenReadTransaction())
            {
                return ValueTask.FromResult(RequestHandler.Database.DocumentsStorage.ConflictsStorage.GetConflictsPreviewResult(context, start, pageSize));
            }
        }

        protected override async Task GetConflictsForDocumentAsync(DocumentsOperationContext context, string documentId)
        {
            using (context.OpenReadTransaction())
            {
                var conflicts = context.DocumentDatabase.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, documentId);
                await WriteDocumentConflicts(context, documentId, conflicts);
            }
        }

        private async Task WriteDocumentConflicts(JsonOperationContext context, string documentId, IEnumerable<DocumentConflict> conflicts)
        {
            long maxEtag = 0;
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                var array = new DynamicJsonArray();

                foreach (var conflict in conflicts)
                {
                    if (maxEtag < conflict.Etag)
                        maxEtag = conflict.Etag;

                    array.Add(new DynamicJsonValue
                    {
                        [nameof(GetConflictsResult.Conflict.ChangeVector)] = conflict.ChangeVector,
                        [nameof(GetConflictsResult.Conflict.Doc)] = conflict.Doc,
                        [nameof(GetConflictsResult.Conflict.LastModified)] = conflict.LastModified
                    });
                }

                context.Write(writer, new DynamicJsonValue
                {
                    [nameof(GetConflictsResult.Id)] = documentId,
                    [nameof(GetConflictsResult.LargestEtag)] = maxEtag,
                    [nameof(GetConflictsResult.Results)] = array
                });
            }
        }
    }
}
