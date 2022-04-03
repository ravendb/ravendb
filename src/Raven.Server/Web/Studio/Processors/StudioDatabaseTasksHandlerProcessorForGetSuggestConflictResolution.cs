using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.Studio.Processors
{
    internal class StudioDatabaseTasksHandlerProcessorForGetSuggestConflictResolution : AbstractStudioDatabaseTasksHandlerProcessorForGetSuggestConflictResolution<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public StudioDatabaseTasksHandlerProcessorForGetSuggestConflictResolution([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async Task GetSuggestConflictResolutionAsync(DocumentsOperationContext context, string documentId)
        {
            using (context.OpenReadTransaction())
            {
                var conflicts = context.DocumentDatabase.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, documentId);

                var advisor = new ConflictResolverAdvisor(conflicts.Select(c => c.Doc), context);
                var resolved = advisor.Resolve();

                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(ConflictResolverAdvisor.MergeResult.Document)] = resolved.Document,
                        [nameof(ConflictResolverAdvisor.MergeResult.Metadata)] = resolved.Metadata
                    });
                }
            }
        }
    }
}
