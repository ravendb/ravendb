using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Admin.Processors.Sorters;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminSortersHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/sorters", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task Put()
        {
            using (var processor = new AdminSortersHandlerProcessorForPut(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/sorters", "DELETE", AuthorizationStatus.DatabaseAdmin)]
        public async Task Delete()
        {
            using (var processor = new AdminSortersHandlerProcessorForDelete(this))
                await processor.ExecuteAsync();
        }
    }
}
