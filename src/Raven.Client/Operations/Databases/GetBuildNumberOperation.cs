using System.Net.Http;
using Raven.Client.Commands;
using Raven.Client.Data;
using Raven.Client.Document;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Operations.Databases
{
    public class GetBuildNumberOperation : IAdminOperation<BuildNumber>
    {
        public RavenCommand<BuildNumber> GetCommand(DocumentConvention conventions, JsonOperationContext context)
        {
            return new GetBuildNumberCommand();
        }

        private class GetBuildNumberCommand : RavenCommand<BuildNumber>
        {
            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/build/version";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.BuildNumber(response);
            }
        }
    }
}