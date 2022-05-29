using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Studio
{
    public class DeleteStudioCollectionOperation : IMaintenanceOperation
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

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteStudioCollectionCommand(_operationId, _collectionName, _excludeIds);
        }

        internal class DeleteStudioCollectionCommand : RavenCommand
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
        }
    }
}
