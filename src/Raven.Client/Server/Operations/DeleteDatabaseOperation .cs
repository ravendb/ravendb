using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Server.Operations
{
    public class DeleteDatabaseOperation : IServerOperation<DeleteDatabaseResult>
    {
        private readonly string _name;
        private readonly bool _hardDelete;
        private readonly string _fromNode;

        public DeleteDatabaseOperation(string name, bool hardDelete,string fromNode = null)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            _name = name;
            _hardDelete = hardDelete;
            _fromNode = fromNode;
        }

        public RavenCommand<DeleteDatabaseResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteDatabaseCommand(_name, _hardDelete, _fromNode);
        }

        private class DeleteDatabaseCommand : RavenCommand<DeleteDatabaseResult>
        {
            private readonly string _name;
            private readonly bool _hardDelete;
            private readonly string _fromNode;
            public DeleteDatabaseCommand(string name, bool hardDelete,string fromNode)
            {
                if (name == null)
                    throw new ArgumentNullException(nameof(name));

                _name = name;
                _hardDelete = hardDelete;
                _fromNode = fromNode;
                ResponseType = RavenCommandResponseType.Object;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/databases?name={_name}";
                if (_hardDelete)
                {
                    url += "&hard-delete=true";
                }
                if (string.IsNullOrEmpty(_fromNode) == false)
                {
                    url += $"&from-node={_fromNode}";
                }
                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Delete
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonDeserializationClient.DeleteDatabaseResult(response);
            }
            /*
            public override void SetResponse(BlittableJsonReaderArray response, bool fromCache)
            {
            }*/
            
            public override bool IsReadRequest => false;
        }
    }
}