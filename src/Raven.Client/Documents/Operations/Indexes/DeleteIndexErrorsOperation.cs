using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class DeleteIndexErrorsOperation : IMaintenanceOperation
    {
        private readonly string[] _indexNames;

        public DeleteIndexErrorsOperation()
        {
        }

        public DeleteIndexErrorsOperation(string[] indexNames)
        {
            _indexNames = indexNames;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteIndexErrorsCommand(_indexNames);
        }

        private class DeleteIndexErrorsCommand : RavenCommand
        {
            private readonly string[] _indexNames;

            public DeleteIndexErrorsCommand(string[] indexNames)
            {
                _indexNames = indexNames;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes/errors";
                if (_indexNames != null && _indexNames.Length > 0)
                {
                    url += "?";
                    foreach (var indexName in _indexNames)
                        url += $"&name={indexName}";
                }

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Delete
                };
            }

            public override bool IsReadRequest => true;
        }
    }
}
