using System.Net.Http;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Queries;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers
{
    public class QueriesHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/queries", "POST", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task Post()
        {
            using (var processor = new DatabaseQueriesHandlerProcessorForGet(this, HttpMethod.Post))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/queries", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task Get()
        {
            using (var processor = new DatabaseQueriesHandlerProcessorForGet(this, HttpMethod.Get))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/queries", "PATCH", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task Patch()
        {
            using (var processor = new DatabaseQueriesHandlerProcessorForPatch(this)) 
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/queries/test", "PATCH", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task PatchTest()
        {
            using (var processor = new DatabaseQueriesHandlerProcessorForPatchTest(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/queries", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Delete()
        {
            using (var processor = new DatabaseQueriesHandlerProcessorForDelete(this))
                await processor.ExecuteAsync();
        }
    }
}
