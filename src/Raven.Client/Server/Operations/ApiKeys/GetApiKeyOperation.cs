using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Server.Operations.ApiKeys
{
    public class GetApiKeyOperation : IServerOperation<NamedApiKeyDefinition>
    {
        private readonly string _name;

        public GetApiKeyOperation(string name)
        {
            _name = name ?? throw new ArgumentNullException(nameof(name));
        }

        public RavenCommand<NamedApiKeyDefinition> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetApiKeyCommand(_name);
        }

        private class GetApiKeyCommand : RavenCommand<NamedApiKeyDefinition>
        {
            private readonly string _name;

            public GetApiKeyCommand(string name)
            {
                _name = name ?? throw new ArgumentNullException(nameof(name));
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/api-keys?name=" + Uri.EscapeDataString(_name);

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                var results = JsonDeserializationClient.GetApiKeysResponse(response).Results;

                if (results.Length != 1)
                    ThrowInvalidResponse();

                Result = results[0];
            }
        }
    }
}