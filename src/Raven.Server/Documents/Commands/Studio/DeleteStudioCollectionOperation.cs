using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Studio
{
    public class DeleteStudioCollectionOperation : IOperation<OperationIdResult>
    {
        private readonly long? _operationId;
        private readonly string _collectionName;
        private readonly List<string> _excludeIds;

        public DeleteStudioCollectionOperation(long? operationId, string collectionName, List<string> excludeIds)
        {
            _operationId = operationId;
            _collectionName = collectionName;
            _excludeIds = excludeIds;
        }

        public RavenCommand<OperationIdResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new DeleteStudioCollectionCommand(_operationId, _collectionName, _excludeIds);
        }

        internal class DeleteStudioCollectionCommand : RavenCommand<OperationIdResult>
        {
            private readonly long? _operationId;
            private readonly string _collectionName;
            private readonly List<string> _excludeIds;

            public DeleteStudioCollectionCommand(long? operationId, string collectionName, List<string> excludeIds)
            {
                _operationId = operationId;
                _collectionName = collectionName;
                _excludeIds = excludeIds;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var path = new StringBuilder(node.Url)
                    .Append("/databases/")
                    .Append(node.Database)
                    .Append("/studio/collections/docs");

                path.Append("?name=").Append(Uri.EscapeDataString(_collectionName));

                if(_operationId.HasValue)
                    path.Append("&operationId=").Append(_operationId);

                url = path.ToString();

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Delete,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteStartObject();
                            writer.WriteArray("ExcludeIds", _excludeIds);
                            writer.WriteEndObject();
                        }
                    })
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if(response == null)
                    return;

                Result = JsonDeserializationClient.OperationIdResult(response);
            }
        }
    }
}
