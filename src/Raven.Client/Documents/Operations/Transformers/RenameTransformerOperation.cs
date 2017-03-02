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
            if (name == null)
                throw new ArgumentNullException(nameof(name));
            if (newName == null)
                throw new ArgumentNullException(nameof(newName));

            _name = name;
            _newName = newName;
        }

        public RavenCommand<object> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new RenameTransformerCommand(_name, _newName);
        }

        private class RenameTransformerCommand : RavenCommand<object>
        {
            private readonly string _name;
            private readonly string _newName;

            public RenameTransformerCommand(string name, string newName)
            {
                if (name == null)
                    throw new ArgumentNullException(nameof(name));

                if(newName == null)
                    throw new ArgumentNullException(nameof(newName));

                _name = name;
                _newName = newName;
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
            }

            public override bool IsReadRequest => false;
        }
    }
}