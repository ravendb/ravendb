using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Sorters
{
    /// <summary>
    /// <para>The Lucene indexing engine allows you to create custom sorters where you can define how query results will be ordered based on your specific requirements.</para>
    /// <para>Use PutSortersOperation to deploy a custom sorter to the RavenDB server.</para>
    /// <para>Once deployed, it can be used to sort query results for all queries made on the database scoped to your Document Store.</para>
    /// </summary>
    public sealed class PutSortersOperation : IMaintenanceOperation
    {
        private readonly SorterDefinition[] _sortersToAdd;

        /// <inheritdoc cref="PutSortersOperation" />
        /// <param name="sortersToAdd">An array of SorterDefinition objects representing the custom sorters to be deployed to the server.</param>
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

        private sealed class PutSortersCommand : RavenCommand, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly BlittableJsonReaderObject[] _sortersToAdd;

            public PutSortersCommand(DocumentConventions conventions, JsonOperationContext context, SorterDefinition[] sortersToAdd)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (sortersToAdd == null)
                    throw new ArgumentNullException(nameof(sortersToAdd));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                _conventions = conventions;

                _sortersToAdd = new BlittableJsonReaderObject[sortersToAdd.Length];

                for (var i = 0; i < sortersToAdd.Length; i++)
                {
                    if (sortersToAdd[i].Name == null)
                        throw new ArgumentNullException(nameof(SorterDefinition.Name));

                    _sortersToAdd[i] = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(sortersToAdd[i], context);
                }
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/sorters";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteStartObject();
                            writer.WriteArray("Sorters", _sortersToAdd);
                            writer.WriteEndObject();
                        }
                    }, _conventions)
                };

                return request;
            }

            public override bool IsReadRequest => false;
            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
