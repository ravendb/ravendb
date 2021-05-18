using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Routing;

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
    }
}
