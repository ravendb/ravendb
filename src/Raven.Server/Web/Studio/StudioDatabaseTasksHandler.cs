using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.Web.Studio.Processors;

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
    }
}
