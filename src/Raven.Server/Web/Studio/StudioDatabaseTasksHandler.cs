using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.Web.Studio.Processors;

namespace Raven.Server.Web.Studio
{
    public class StudioDatabaseTasksHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/studio-tasks/folder-path-options", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task GetFolderPathOptionsForDatabaseAdmin()
        {
            using (var processor = new StudioStudioDatabaseTasksHandlerProcessorForGetFolderPathOptionsForDatabaseAdmin(this))
                await processor.ExecuteAsync();
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
            using (var processor = new StudioDatabaseTasksHandlerProcessorForGetSuggestConflictResolution(this))
                await processor.ExecuteAsync();
        }
    }
}
