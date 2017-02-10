using System;
using System.Net.Http;
using Raven.Client.Commands;
using Raven.Client.Document;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Operations.Databases
{
    public class DeleteDatabaseOperation : IAdminOperation
    {
        private readonly string _name;
        private readonly bool _hardDelete;

        public DeleteDatabaseOperation(string name, bool hardDelete)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            _name = name;
            _hardDelete = hardDelete;
        }

        public RavenCommand<object> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteDatabaseCommand(_name, _hardDelete);
        }

        private class DeleteDatabaseCommand : RavenCommand<object>
        {
            private readonly string _name;
            private readonly bool _hardDelete;

            public DeleteDatabaseCommand(string name, bool hardDelete)
            {
                if (name == null)
                    throw new ArgumentNullException(nameof(name));

                _name = name;
                _hardDelete = hardDelete;
                ResponseType = RavenCommandResponseType.Array;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/databases?name={_name}";
                if (_hardDelete)
                    url += "&hard-delete=true";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Delete
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                ThrowInvalidResponse();
            }

            public override void SetResponse(BlittableJsonReaderArray response, bool fromCache)
            {
            }

            public override bool IsReadRequest => false;
        }
    }
}