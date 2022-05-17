using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Analyzers;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminAnalyzersHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/analyzers", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task Put()
        {
            using (var processor = new AdminAnalyzersHandlerProcessorForPut(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/analyzers", "DELETE", AuthorizationStatus.DatabaseAdmin)]
        public async Task Delete()
        {
            using (var processor = new AdminAnalyzersHandlerProcessorForDelete(this))
                await processor.ExecuteAsync();
        }
    }
}
