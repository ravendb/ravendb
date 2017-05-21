using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Server.Operations.ApiKeys
{
    public class DeleteApiKeyOperation : IServerOperation
    {
        private readonly string _name;

        public DeleteApiKeyOperation(string name)
        {
            _name = name ?? throw new ArgumentNullException(nameof(name));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteApiKeyCommand(_name);
        }

        private class DeleteApiKeyCommand : RavenCommand
        {
            private readonly string _name;

            public DeleteApiKeyCommand(string name)
            {
                _name = name ?? throw new ArgumentNullException(nameof(name));
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/api-keys?name=" + Uri.EscapeDataString(_name);

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Delete
                };
            }
        }
    }
}