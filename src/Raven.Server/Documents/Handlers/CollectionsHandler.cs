using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Collections;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class CollectionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/collections/stats", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetCollectionStats()
        {
            using (var processor = new CollectionsHandlerProcessorForGetCollectionStats(this, detailed: false))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/collections/stats/detailed", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetDetailedCollectionStats()
        {
            using (var processor = new CollectionsHandlerProcessorForGetCollectionStats(this, detailed: true))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/collections/docs", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetCollectionDocuments()
        {
            using (var processor = new CollectionsHandlerProcessorForGetCollectionDocuments(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/collections/last-change-vector", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetLastDocumentChangeVectorForCollection()
        {
            using (var processor = new CollectionsHandlerProcessorForGetLastChangeVector(this))
                await processor.ExecuteAsync();
        }
    }
}
