using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class DatabaseHealthCheckOperation : IMaintenanceOperation
    {
        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DatabaseHealthCheckCommand();
        }

        private class DatabaseHealthCheckCommand : RavenCommand
        {
            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/is-loaded";
                return new HttpRequestMessage { Method = HttpMethod.Get };
            }
        }
    }
}