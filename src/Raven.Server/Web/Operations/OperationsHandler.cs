using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.Web.Operations.Processors;

namespace Raven.Server.Web.Operations
{
    public class OperationsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/operations/next-operation-id", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetNextOperationId()
        {
            using (var processor = new OperationsHandlerProcessorForGetNextOperationId(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/operations/kill", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Kill()
        {
            using (var processor = new OperationsHandlerProcessorForKill(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/operations", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetAll()
        {
            using (var processor = new OperationsHandlerProcessorForGetAll(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/operations/state", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task State()
        {
            using (var processor = new OperationsHandlerProcessorForState(this))
                await processor.ExecuteAsync();
        }
    }
}
