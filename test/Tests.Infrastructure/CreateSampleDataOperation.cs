using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Sparrow.Json;

namespace Tests.Infrastructure
{
    public class CreateSampleDataOperation : IAdminOperation
    {
        public RavenCommand<object> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new CreateSampleDataCommand();
        }

        private class CreateSampleDataCommand : RavenCommand<object>
        {
            public CreateSampleDataCommand()
            {
                ResponseType = RavenCommandResponseType.Empty;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/studio/sample-data";

                ResponseType = RavenCommandResponseType.Object;
                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }
        }
    }
}