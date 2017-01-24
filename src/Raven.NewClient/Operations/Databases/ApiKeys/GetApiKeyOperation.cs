using System;
using System.Net.Http;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Sparrow.Json;

namespace Raven.NewClient.Operations.Databases.ApiKeys
{
    public class GetApiKeyOperation : IAdminOperation<NamedApiKeyDefinition>
    {
        private readonly string _name;

        public GetApiKeyOperation(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            _name = name;
        }

        public RavenCommand<NamedApiKeyDefinition> GetCommand(DocumentConvention conventions, JsonOperationContext context)
        {
            return new GetApiKeyCommand(_name);
        }

        private class GetApiKeyCommand : RavenCommand<NamedApiKeyDefinition>
        {
            private readonly string _name;

            public GetApiKeyCommand(string name)
            {
                if (name == null)
                    throw new ArgumentNullException(nameof(name));

                _name = name;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/api-keys?name=" + Uri.EscapeUriString(_name);

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