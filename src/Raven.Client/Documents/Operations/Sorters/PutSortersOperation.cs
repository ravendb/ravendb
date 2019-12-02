using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
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

        private class PutSortersCommand : RavenCommand, IRaftCommand
        {
            private readonly SorterDefinition[] _sortersToAdd;

            public PutSortersCommand(DocumentConventions conventions, JsonOperationContext context, SorterDefinition[] sortersToAdd)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (sortersToAdd == null)
                    throw new ArgumentNullException(nameof(sortersToAdd));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));

                for (var i = 0; i < sortersToAdd.Length; i++)
                {
                    if (sortersToAdd[i].Name == null)
                        throw new ArgumentNullException(nameof(SorterDefinition.Name));
                }

                _sortersToAdd = sortersToAdd;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/sorters";

                var sorters = new BlittableJsonReaderObject[_sortersToAdd.Length];
                for (var index = 0; index < _sortersToAdd.Length; index++)
                {
                    sorters[index] = EntityToBlittable.ConvertCommandToBlittable(_sortersToAdd[index], ctx);
                }

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(this, stream =>
                    {
                        using (var writer = new BlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteStartObject();
                            writer.WriteArray("Sorters", sorters);
                            writer.WriteEndObject();
                        }
                    })
                };

                return request;
            }

            public override bool IsReadRequest => false;
            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
