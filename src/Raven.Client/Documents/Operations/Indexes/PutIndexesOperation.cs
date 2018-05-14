using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
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

        private class PutIndexesCommand : RavenCommand<PutIndexResult[]>
        {
            private readonly BlittableJsonReaderObject[] _indexToAdd;

            public PutIndexesCommand(DocumentConventions conventions, JsonOperationContext context, IndexDefinition[] indexesToAdd)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (indexesToAdd == null)
                    throw new ArgumentNullException(nameof(indexesToAdd));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));

                _indexToAdd = new BlittableJsonReaderObject[indexesToAdd.Length];
                for (var i = 0; i < indexesToAdd.Length; i++)
                {
                    if (indexesToAdd[i].Name == null)
                        throw new ArgumentNullException(nameof(IndexDefinition.Name));
                    _indexToAdd[i] = EntityToBlittable.ConvertCommandToBlittable(indexesToAdd[i], context);
                }
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/indexes";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(stream =>
                    {
                        using (var writer = new BlittableJsonTextWriter(ctx, stream))
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
        }
    }
}