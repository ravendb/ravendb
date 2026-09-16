using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.ElasticSearch.Handlers.Processors;
using Raven.Server.Routing;
using Raven.Server.Web;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch.Handlers
{
    public sealed class ElasticSearchEtlServerWideConnectionHandler : ServerRequestHandler
    {
        [RavenAction("/admin/etl/elasticsearch/test-connection", "POST", AuthorizationStatus.Operator)]
        public Task TestConnection() => ElasticSearchEtlTestConnectionHelper.ExecuteAsync(this);
    }
}
