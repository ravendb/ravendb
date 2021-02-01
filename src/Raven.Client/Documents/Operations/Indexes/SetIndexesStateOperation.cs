using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class SetIndexesStateOperation : IMaintenanceOperation
    {
        private readonly Parameters _parameters;

        public SetIndexesStateOperation(string indexName, IndexState state)
        {
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));

            _parameters = new Parameters
            {
                IndexNames = new[] { indexName },
                State = state
            };
        }

        public SetIndexesStateOperation(Parameters parameters)
        {
            if (parameters == null)
                throw new ArgumentNullException(nameof(parameters));

            if (parameters.IndexNames == null || parameters.IndexNames.Length == 0)
                throw new ArgumentNullException(nameof(parameters.IndexNames));

            _parameters = parameters;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new SetIndexStateCommand(conventions, context, _parameters);
        }

        private class SetIndexStateCommand : RavenCommand, IRaftCommand
        {
            private readonly BlittableJsonReaderObject _parameters;

            public SetIndexStateCommand(DocumentConventions conventions, JsonOperationContext context, Parameters parameters)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                if (parameters == null)
                    throw new ArgumentNullException(nameof(parameters));

                _parameters = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(parameters, context);
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/indexes/set-state";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        ctx.Write(stream, _parameters);
                    })
                };
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }

        public class Parameters
        {
            public string[] IndexNames { get; set; }
            public IndexState State { get; set; }
        }
    }
}
