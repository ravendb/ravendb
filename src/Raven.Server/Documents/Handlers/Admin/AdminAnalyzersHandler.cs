using System.Net;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Analyzers;
using Raven.Server.Documents.Indexes.Analysis;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands.Analyzers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

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
