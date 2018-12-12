using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Sorters
{
    public class PutSortersOperation : IMaintenanceOperation
    {
        private readonly SorterDefinition[] _sortersToAdd;

        public PutSortersOperation(params SorterDefinition[] sortersToAdd)
        {
            if (sortersToAdd == null || sortersToAdd.Length == 0)
                throw new ArgumentNullException(nameof(sortersToAdd));

            _sortersToAdd = sortersToAdd;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new PutSortersCommand(conventions, context, _sortersToAdd);
        }

        private class PutSortersCommand : RavenCommand
        {
            private readonly BlittableJsonReaderObject[] _sortersToAdd;

            public PutSortersCommand(DocumentConventions conventions, JsonOperationContext context, SorterDefinition[] sortersToAdd)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (sortersToAdd == null)
                    throw new ArgumentNullException(nameof(sortersToAdd));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));

                _sortersToAdd = new BlittableJsonReaderObject[sortersToAdd.Length];

                for (var i = 0; i < sortersToAdd.Length; i++)
                {
                    if (sortersToAdd[i].Name == null)
                        throw new ArgumentNullException(nameof(SorterDefinition.Name));

                    _sortersToAdd[i] = EntityToBlittable.ConvertCommandToBlittable(sortersToAdd[i], context);
                }
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/sorters";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(stream =>
                    {
                        using (var writer = new BlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteStartObject();
                            writer.WriteArray("Sorters", _sortersToAdd);
                            writer.WriteEndObject();
                        }
                    })
                };

                return request;
            }

            public override bool IsReadRequest => false;
        }
    }
}
