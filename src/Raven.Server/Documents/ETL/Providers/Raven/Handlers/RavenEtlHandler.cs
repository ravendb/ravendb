using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Server.Documents.ETL.Providers.Raven.Test;
using Raven.Server.Documents.Handlers.Processors.ETL;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.Raven.Handlers
{
    public class RavenEtlHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/etl/raven/test", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task PostScriptTest()
        {
            using (var processor = new EtlHandlerProcessorForTestEtl(this))
                await processor.ExecuteAsync();
        }
    }
}
