using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class PatchByQueryOperation : IOperation<OperationIdResult>
    {
        protected static IndexQuery DummyQuery = new IndexQuery();

        private readonly IndexQuery _queryToUpdate;
        private readonly QueryOperationOptions _options;

        public PatchByQueryOperation(string queryToUpdate)
            : this(new IndexQuery { Query = queryToUpdate })
        {
        }

        public PatchByQueryOperation(IndexQuery queryToUpdate, QueryOperationOptions options = null)
        {
            _queryToUpdate = queryToUpdate ?? throw new ArgumentNullException(nameof(queryToUpdate));
            _options = options;
        }

        public virtual RavenCommand<OperationIdResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new PatchByQueryCommand(conventions, context, _queryToUpdate, _options);
        }

        private class PatchByQueryCommand : RavenCommand<OperationIdResult>
        {
            private readonly DocumentConventions _conventions;
            private readonly IndexQuery _queryToUpdate;
            private readonly QueryOperationOptions _options;

            public PatchByQueryCommand(DocumentConventions conventions, JsonOperationContext context,
                IndexQuery queryToUpdate,
                QueryOperationOptions options = null)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                _queryToUpdate = queryToUpdate ?? throw new ArgumentNullException(nameof(queryToUpdate));
                _options = options ?? new QueryOperationOptions();
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var path = new StringBuilder(node.Url)
                    .Append("/databases/")
                    .Append(node.Database)
                    .Append("/queries")
                    .Append("?allowStale=")
                    .Append(_options.AllowStale)
                    .Append("&maxOpsPerSec=")
                    .Append(_options.MaxOpsPerSecond)
                    .Append("&details=")
                    .Append(_options.RetrieveDetails);

                if (_options.StaleTimeout != null)
                {
                    path
                        .Append("&staleTimeout=")
                        .Append(_options.StaleTimeout.Value);
                }

                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Patch,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteStartObject();

                            writer.WritePropertyName("Query");
                            writer.WriteIndexQuery(_conventions, ctx, _queryToUpdate);

                            writer.WriteEndObject();
                        }
                    })
                };

                url = path.ToString();
                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.OperationIdResult(response);
            }

            public override bool IsReadRequest => false;
        }
    }
}
