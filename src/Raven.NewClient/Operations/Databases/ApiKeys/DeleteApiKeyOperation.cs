using System;
using System.Net.Http;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Sparrow.Json;

namespace Raven.NewClient.Operations.Databases.ApiKeys
{
    public class DeleteApiKeyOperation : IAdminOperation
    {
        private readonly string _name;

        public DeleteApiKeyOperation(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            _name = name;
        }

        public RavenCommand<object> GetCommand(DocumentConvention conventions, JsonOperationContext context)
        {
            return new DeleteApiKeyCommand(_name);
        }

        private class DeleteApiKeyCommand : RavenCommand<object>
        {
            private readonly string _name;

            public DeleteApiKeyCommand(string name)
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
                    Method = HttpMethod.Delete
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
            }
        }
    }
}