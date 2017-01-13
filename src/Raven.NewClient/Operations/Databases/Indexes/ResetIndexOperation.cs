using System;
using System.Net.Http;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Sparrow.Json;

namespace Raven.NewClient.Operations.Databases.Indexes
{
    public class ResetIndexOperation : IAdminOperation
    {
        private readonly string _indexName;

        public ResetIndexOperation(string indexName)
        {
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));

            _indexName = indexName;
        }

        public RavenCommand<object> GetCommand(DocumentConvention conventions)
        {
            return new ResetIndexCommand(_indexName);
        }

        private class ResetIndexCommand : RavenCommand<object>
        {
            private readonly string _indexName;

            public ResetIndexCommand(string indexName)
            {
                if (indexName == null)
                    throw new ArgumentNullException(nameof(indexName));

                _indexName = indexName;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes?name={Uri.EscapeUriString(_indexName)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethods.Reset
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response)
            {
            }
        }
    }
}