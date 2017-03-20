using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class RenameIndexOperation : IAdminOperation
    {
        private readonly string _name;
        private readonly string _newName;

        public RenameIndexOperation(string name, string newName)
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
            return new RenameIndexCommand(_name, _newName);
        }

        private class RenameIndexCommand : RavenCommand<object>
        {
            private readonly string _name;
            private readonly string _newName;

            public RenameIndexCommand(string name, string newName)
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
                url = $"{node.Url}/databases/{node.Database}/indexes/rename?name={Uri.EscapeDataString(_name)}&newName={Uri.EscapeDataString(_newName)}";

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