using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Server.Operations.ApiKeys
{
    public class PutApiKeyOperation : IServerOperation
    {
        private readonly string _name;
        private readonly ApiKeyDefinition _apiKey;

        public PutApiKeyOperation(string name, ApiKeyDefinition apiKey)
        {
            _name = name ?? throw new ArgumentNullException(nameof(name));
            _apiKey = apiKey ?? throw new ArgumentNullException(nameof(apiKey));
        }

        public RavenCommand<object> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new PutApiKeyCommand(conventions, context, _name, _apiKey);
        }

        private class PutApiKeyCommand : RavenCommand<object>
        {
            private readonly JsonOperationContext _context;
            private readonly string _name;
            private readonly BlittableJsonReaderObject _apiKey;

            public PutApiKeyCommand(DocumentConventions conventions, JsonOperationContext context, string name, ApiKeyDefinition apiKey)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (apiKey == null)
                    throw new ArgumentNullException(nameof(apiKey));

                _context = context ?? throw new ArgumentNullException(nameof(context));
                _name = name ?? throw new ArgumentNullException(nameof(name));
                _apiKey = EntityToBlittable.ConvertEntityToBlittable(apiKey, conventions, context);
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/api-keys?name=" + Uri.EscapeDataString(_name);

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(stream =>
                    {
                        _context.Write(stream, _apiKey);
                    })
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
            }
        }
    }
}