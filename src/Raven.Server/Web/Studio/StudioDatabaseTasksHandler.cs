using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Studio.Processors;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.Studio
{
    public class StudioDatabaseTasksHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/studio-tasks/folder-path-options", "POST", AuthorizationStatus.DatabaseAdmin)]
        public Task GetFolderPathOptionsForDatabaseAdmin()
        {
            var type = GetStringValuesQueryString("type", required: false);
            var isBackupFolder = GetBoolValueQueryString("backupFolder", required: false) ?? false;
            var path = GetStringQueryString("path", required: false);

            return StudioTasksHandler.GetFolderPathOptionsInternal(ServerStore, type, isBackupFolder, path, RequestBodyStream, ResponseBodyStream);
        }

        [RavenAction("/databases/*/studio-tasks/indexes/configuration/defaults", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetIndexDefaults()
        {
            using (var processor = new StudioDatabaseTasksHandlerProcessorForGetIndexDefaults(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/studio-tasks/suggest-conflict-resolution", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task SuggestConflictResolution()
        {
            var docId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("docId");
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            using (context.OpenReadTransaction())
            {
                var conflicts = context.DocumentDatabase.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, docId);
                var advisor = new ConflictResolverAdvisor(conflicts.Select(c => c.Doc), context);
                var resolved = advisor.Resolve();

                context.Write(writer, new DynamicJsonValue
                {
                    [nameof(ConflictResolverAdvisor.MergeResult.Document)] = resolved.Document,
                    [nameof(ConflictResolverAdvisor.MergeResult.Metadata)] = resolved.Metadata
                });
            }
        }
    }
}
