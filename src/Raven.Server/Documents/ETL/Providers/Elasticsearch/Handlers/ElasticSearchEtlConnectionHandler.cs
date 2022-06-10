using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.ElasticSearch.Handlers.Processors;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch.Handlers
{
    public class ElasticSearchEtlConnectionHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/etl/elasticsearch/test-connection", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task TestConnection()
        {
            using (var processor = new ElasticSearchEtlConnectionHandlerForTestConnection<DocumentsOperationContext>(this))
                await processor.ExecuteAsync();
        }
    }
}
