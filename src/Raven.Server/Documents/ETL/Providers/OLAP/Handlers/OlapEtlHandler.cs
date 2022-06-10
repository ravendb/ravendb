using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.OLAP.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.ETL.Providers.OLAP.Handlers
{
    public class OlapEtlHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/etl/olap/test", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task PostScriptTest()
        {
            using (var processor = new OlapEtlHandlerProcessorForTest(this))
                await processor.ExecuteAsync();
        }
    }
}
