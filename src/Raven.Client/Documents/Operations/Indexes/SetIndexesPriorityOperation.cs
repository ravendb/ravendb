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
    public class SetIndexesPriorityOperation : IMaintenanceOperation
    {
        private readonly Parameters _parameters;

        public SetIndexesPriorityOperation(string indexName, IndexPriority priority)
        {
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));

            _parameters = new Parameters
            {
                IndexNames = new[] { indexName },
                Priority = priority
            };
        }

        public SetIndexesPriorityOperation(Parameters parameters)
        {
            if (parameters == null)
                throw new ArgumentNullException(nameof(parameters));

            if (parameters.IndexNames == null || parameters.IndexNames.Length == 0)
                throw new ArgumentNullException(nameof(parameters.IndexNames));

            _parameters = parameters;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new SetIndexPriorityCommand(conventions, context, _parameters);
        }

        private class SetIndexPriorityCommand : RavenCommand, IRaftCommand
        {
            private readonly BlittableJsonReaderObject _parameters;

            public SetIndexPriorityCommand(DocumentConventions conventions, JsonOperationContext context, Parameters parameters)
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
                url = $"{node.Url}/databases/{node.Database}/indexes/set-priority";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _parameters).ConfigureAwait(false))
                };
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }

        public class Parameters
        {
            public string[] IndexNames { get; set; }
            public IndexPriority Priority { get; set; }
        }
    }
}
