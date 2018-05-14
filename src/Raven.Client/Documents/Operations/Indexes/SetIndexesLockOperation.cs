using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class SetIndexesLockOperation : IMaintenanceOperation
    {
        private readonly Parameters _parameters;

        public SetIndexesLockOperation(string indexName, IndexLockMode mode)
        {
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));

            _parameters = new Parameters
            {
                IndexNames = new[] { indexName },
                Mode = mode
            };
        }

        public SetIndexesLockOperation(Parameters parameters)
        {
            if (parameters == null)
                throw new ArgumentNullException(nameof(parameters));

            if (parameters.IndexNames == null || parameters.IndexNames.Length == 0)
                throw new ArgumentNullException(nameof(parameters.IndexNames));

            _parameters = parameters;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new SetIndexLockCommand(conventions, context, _parameters);
        }

        private class SetIndexLockCommand : RavenCommand
        {
            private readonly BlittableJsonReaderObject _parameters;

            public SetIndexLockCommand(DocumentConventions conventions, JsonOperationContext context, Parameters parameters)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                if (parameters == null)
                    throw new ArgumentNullException(nameof(parameters));

                _parameters = EntityToBlittable.ConvertCommandToBlittable(parameters, context);
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes/set-lock";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        ctx.Write(stream, _parameters);
                    })
                };
            }
        }

        public class Parameters
        {
            public string[] IndexNames { get; set; }
            public IndexLockMode Mode { get; set; }
        }
    }
}
