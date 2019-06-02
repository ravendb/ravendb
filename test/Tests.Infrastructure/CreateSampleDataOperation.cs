using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Tests.Infrastructure
{
    public class CreateSampleDataOperation : IMaintenanceOperation
    {
        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new CreateSampleDataCommand();
        }

        private class CreateSampleDataCommand : RavenCommand, IRaftCommand
        {
            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/studio/sample-data";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
