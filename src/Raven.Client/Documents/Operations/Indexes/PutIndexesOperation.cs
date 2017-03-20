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
    public class PutIndexesOperation : IAdminOperation<PutIndexResult[]>
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
            private readonly JsonOperationContext _context;
            private readonly BlittableJsonReaderObject[] _indexToAdd;

            public PutIndexesCommand(DocumentConventions conventions, JsonOperationContext context, IndexDefinition[] indexesToAdd)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                if (indexesToAdd == null)
                    throw new ArgumentNullException(nameof(indexesToAdd));

                _context = context;
                _indexToAdd = new BlittableJsonReaderObject[indexesToAdd.Length];
                for (var i = 0; i < indexesToAdd.Length; i++)
                {
                    if (indexesToAdd[i].Name == null)
                        throw new ArgumentNullException(nameof(IndexDefinition.Name));
                    _indexToAdd[i] = EntityToBlittable.ConvertEntityToBlittable(indexesToAdd[i], conventions, _context);
                }
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(stream =>
                    {
                        using (var writer = new BlittableJsonTextWriter(_context, stream))
                        {
                            writer.WriteStartArray();
                            var first = true;
                            foreach (var index in _indexToAdd)
                            {
                                if (first == false)
                                    writer.WriteComma();
                                first = false;

                                writer.WriteObject(index);
                            }
                            writer.WriteEndArray();
                        }
                    })
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonDeserializationClient.PutIndexesResponse(response).Results;
            }

            public override bool IsReadRequest => false;
        }
    }
}