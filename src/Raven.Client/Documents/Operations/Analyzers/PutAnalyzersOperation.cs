using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes.Analysis;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Analyzers
{
    public class PutAnalyzersOperation : IMaintenanceOperation
    {
        private readonly AnalyzerDefinition[] _analyzersToAdd;

        public PutAnalyzersOperation(params AnalyzerDefinition[] analyzersToAdd)
        {
            if (analyzersToAdd == null || analyzersToAdd.Length == 0)
                throw new ArgumentNullException(nameof(analyzersToAdd));

            _analyzersToAdd = analyzersToAdd;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new PutAnalyzersCommand(conventions, context, _analyzersToAdd);
        }

        private class PutAnalyzersCommand : RavenCommand, IRaftCommand
        {
            private readonly BlittableJsonReaderObject[] _analyzersToAdd;

            public PutAnalyzersCommand(DocumentConventions conventions, JsonOperationContext context, AnalyzerDefinition[] analyzersToAdd)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (analyzersToAdd == null)
                    throw new ArgumentNullException(nameof(analyzersToAdd));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));

                _analyzersToAdd = new BlittableJsonReaderObject[analyzersToAdd.Length];

                for (var i = 0; i < analyzersToAdd.Length; i++)
                {
                    if (analyzersToAdd[i].Name == null)
                        throw new ArgumentNullException(nameof(AnalyzerDefinition.Name));

                    _analyzersToAdd[i] = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(analyzersToAdd[i], context);
                }
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/analyzers";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(stream =>
                    {
                        using (var writer = new BlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteStartObject();
                            writer.WriteArray("Analyzers", _analyzersToAdd);
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
