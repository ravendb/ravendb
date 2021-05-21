using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

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
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var autoIndexesDeploymentMode = Database.Configuration.Indexing.AutoIndexDeploymentMode;
                var staticIndexesDeploymentMode = Database.Configuration.Indexing.StaticIndexDeploymentMode;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName(nameof(IndexDefaults.AutoIndexDeploymentMode));
                    writer.WriteString(autoIndexesDeploymentMode.ToString());
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(IndexDefaults.StaticIndexDeploymentMode));
                    writer.WriteString(staticIndexesDeploymentMode.ToString());
                    writer.WriteEndObject();
                }
            }
        }

        public class IndexDefaults
        {
            public IndexDeploymentMode AutoIndexDeploymentMode { get; set; }
            public IndexDeploymentMode StaticIndexDeploymentMode { get; set; }
        }
    }
}
