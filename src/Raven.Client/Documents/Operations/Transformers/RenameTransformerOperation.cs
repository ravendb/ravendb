using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Transformers
{
    public class RenameTransformerOperation : IAdminOperation
    {
        private readonly string _name;
        private readonly string _newName;

        public RenameTransformerOperation(string name, string newName)
        {
            _name = name ?? throw new ArgumentNullException(nameof(name));
            _newName = newName ?? throw new ArgumentNullException(nameof(newName));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new RenameTransformerCommand(_name, _newName);
        }

        private class RenameTransformerCommand : RavenCommand
        {
            private readonly string _name;
            private readonly string _newName;

            public RenameTransformerCommand(string name, string newName)
            {
                _name = name ?? throw new ArgumentNullException(nameof(name));
                _newName = newName ?? throw new ArgumentNullException(nameof(newName));
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/transformers/rename?name={Uri.EscapeUriString(_name)}&newName={Uri.EscapeUriString(_newName)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }
        }
    }
}