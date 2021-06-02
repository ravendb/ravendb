using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class PutIndexesOperation : IMaintenanceOperation<PutIndexResult[]>
    {
        private readonly IndexDefinition[] _indexToAdd;

        public PutIndexesOperation(params IndexDefinition[] indexToAdd)
        {
            if (indexToAdd == null || indexToAdd.Length == 0)
                throw new ArgumentNullException(nameof(indexToAdd));

            _indexToAdd = indexToAdd;
        }

        public RavenCommand<PutIndexResult[]> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new PutIndexesCommand(conventions, context, _indexToAdd);
        }

        private class PutIndexesCommand : RavenCommand<PutIndexResult[]>, IRaftCommand
        {
            private readonly BlittableJsonReaderObject[] _indexToAdd;
            private bool _allJavaScriptIndexes;

            public PutIndexesCommand(DocumentConventions conventions, JsonOperationContext context, IndexDefinition[] indexesToAdd)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (indexesToAdd == null)
                    throw new ArgumentNullException(nameof(indexesToAdd));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));

                _indexToAdd = new BlittableJsonReaderObject[indexesToAdd.Length];
                _allJavaScriptIndexes = true;
                for (var i = 0; i < indexesToAdd.Length; i++)
                {
                    //We validate on the server that it is indeed a javascript index.
                    if (indexesToAdd[i].Type.IsJavaScript() == false)
                        _allJavaScriptIndexes = false;

                    if (indexesToAdd[i].Name == null)
                        throw new ArgumentNullException(nameof(IndexDefinition.Name));
                    _indexToAdd[i] = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(indexesToAdd[i], context);
                }
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}" + (_allJavaScriptIndexes ? "/indexes" : "/admin/indexes");

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteStartObject();
                            writer.WriteArray("Indexes", _indexToAdd);
                            writer.WriteEndObject();
                        }
                    })
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonDeserializationClient.PutIndexesResponse(response).Results;
            }

            public override bool IsReadRequest => false;
            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
