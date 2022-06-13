using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.System.Processors.Tcp;

namespace Raven.Server.Web.System
{
    public class DatabaseTcpConnectionInfoHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/info/tcp", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task Get()
        {
            using (var processor = new DatabaseTcpConnectionInfoHandlerProcessorForGet<DocumentsOperationContext>(this))
                await processor.ExecuteAsync();
        }
    }
}
