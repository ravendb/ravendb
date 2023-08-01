using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Revisions
{
    public class DeleteRevisionsOperation : IMaintenanceOperation
    {
        private readonly bool _includeForceCreated;
        private readonly Parameters _parameters;

        public DeleteRevisionsOperation(bool includeForceCreated, Parameters parameters) : this(parameters)
        {
            _includeForceCreated = includeForceCreated;
        }

        public DeleteRevisionsOperation(Parameters parameters)
        {
            _parameters = parameters;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteRevisionsCommand(_includeForceCreated, conventions, context, _parameters);
        }

        internal class DeleteRevisionsCommand : RavenCommand
        {
            private readonly bool _includeForceCreated;
            private readonly DocumentConventions _conventions;
            private readonly BlittableJsonReaderObject _parameters;

            public DeleteRevisionsCommand(bool includeForceCreated, DocumentConventions conventions, JsonOperationContext context, Parameters parameters)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                if (parameters == null)
                    throw new ArgumentNullException(nameof(parameters));

                _includeForceCreated = includeForceCreated;
                _conventions = conventions;

                _parameters = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(parameters, context);
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/revisions?includeForceCreated={_includeForceCreated}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Delete,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _parameters).ConfigureAwait(false), _conventions)
                };
            }
        }

        public class Parameters
        {
            public string[] DocumentIds { get; set; }
        }
    }
}
