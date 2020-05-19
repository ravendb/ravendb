using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class GetBuildNumberOperation : IServerOperation<BuildNumber>
    {
        public RavenCommand<BuildNumber> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new GetBuildNumberCommand();
        }

        private class GetBuildNumberCommand : RavenCommand<BuildNumber>
        {
            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/build/version";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.BuildNumber(response);
            }
        }
    }
}