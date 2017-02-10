using System;
using System.Net.Http;
using Raven.Client.Blittable;
using Raven.Client.Commands;
using Raven.Client.Data;
using Raven.Client.Document;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Operations.Databases.Indexes
{
    public class PutIndexesOperation : IAdminOperation<BlittableArrayResult>
    {
        private readonly IndexToAdd[] _indexToAdd;

        public PutIndexesOperation(IndexToAdd[] indexToAdd)
        {
            if (indexToAdd == null)
                throw new ArgumentNullException(nameof(indexToAdd));

            _indexToAdd = indexToAdd;
        }

        public RavenCommand<BlittableArrayResult> GetCommand(DocumentConvention conventions, JsonOperationContext context)
        {
            return new PutIndexesCommand(conventions, context, _indexToAdd);
        }

        private class PutIndexesCommand : RavenCommand<BlittableArrayResult>
        {
            private readonly JsonOperationContext _context;
            private readonly BlittableJsonReaderObject[] _indexToAdd;

            public PutIndexesCommand(DocumentConvention conventions, JsonOperationContext context, IndexToAdd[] indexesToAdd)
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
                    _indexToAdd[i] = new EntityToBlittable(null).ConvertEntityToBlittable(indexesToAdd[i], conventions, _context);
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
                            writer.WriteStartObject();
                            writer.WritePropertyName("Indexes");
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
                            writer.WriteEndObject();
                        }
                    })
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonDeserializationClient.BlittableArrayResult(response);
            }

            public override bool IsReadRequest => false;
        }
    }
}