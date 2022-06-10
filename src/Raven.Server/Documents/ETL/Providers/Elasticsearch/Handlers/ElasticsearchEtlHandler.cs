using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.ElasticSearch.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch.Handlers
{
    public class ElasticSearchEtlHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/etl/elasticsearch/test", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task Test()
        {
            using (var processor = new ElasticSearchEtlHandlerProcessorForTest(this))
                await processor.ExecuteAsync();
        }
    }
}
