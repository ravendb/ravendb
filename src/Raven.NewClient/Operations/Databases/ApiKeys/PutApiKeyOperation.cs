using System;
using System.Net.Http;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Sparrow.Json;

namespace Raven.NewClient.Operations.Databases.ApiKeys
{
    public class PutApiKeyOperation : IAdminOperation
    {
        private readonly string _name;
        private readonly ApiKeyDefinition _apiKey;

        public PutApiKeyOperation(string name, ApiKeyDefinition apiKey)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));
            if (apiKey == null)
                throw new ArgumentNullException(nameof(apiKey));

            _name = name;
            _apiKey = apiKey;
        }

        public RavenCommand<object> GetCommand(DocumentConvention conventions, JsonOperationContext context)
        {
            return new PutApiKeyCommand(conventions, context, _name, _apiKey);
        }

        private class PutApiKeyCommand : RavenCommand<object>
        {
            private readonly JsonOperationContext _context;
            private readonly string _name;
            private readonly BlittableJsonReaderObject _apiKey;

            public PutApiKeyCommand(DocumentConvention conventions, JsonOperationContext context, string name, ApiKeyDefinition apiKey)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                if (name == null)
                    throw new ArgumentNullException(nameof(name));
                if (apiKey == null)
                    throw new ArgumentNullException(nameof(apiKey));

                _context = context;
                _name = name;
                _apiKey = new EntityToBlittable(null).ConvertEntityToBlittable(apiKey, conventions, context);
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/api-keys?name=" + Uri.EscapeUriString(_name);

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