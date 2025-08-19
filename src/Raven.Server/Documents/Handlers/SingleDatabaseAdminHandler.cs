using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Web.Studio.Processors;

namespace Raven.Server.Documents.Handlers
{
    public sealed class SingleDatabaseAdminHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/restart", "POST", AuthorizationStatus.DatabaseAdmin, SkipUsagesCount = true)]
        public async Task RestartDatabase()
        {
            using (var processor = new DatabaseTasksHandlerProcessorForRestartDatabase(this, "/admin/restart"))
                await processor.ExecuteAsync();
        }
    }
}
