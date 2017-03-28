using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Transformers
{
    public class RenameTransformerOperation : IAdminOperation<long>
    {
        private readonly string _name;
        private readonly string _newName;

        public RenameTransformerOperation(string name, string newName)
        {
            _name = name ?? throw new ArgumentNullException(nameof(name));
            _newName = newName ?? throw new ArgumentNullException(nameof(newName));
        }

        public RavenCommand<long> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new RenameTransformerCommand(_name, _newName);
        }

        private class RenameTransformerCommand : RavenCommand<long>
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

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if(response.TryGet("Etag", out long etag) == false)
                    throw new InvalidOperationException("Expected to get Etag from response");
                Result = etag;
            }

            public override bool IsReadRequest => false;
        }
    }
}