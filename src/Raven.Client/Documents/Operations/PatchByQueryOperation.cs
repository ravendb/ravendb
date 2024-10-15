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
    /// <summary>
    ///     A class for creating operations that update documents according to a given patch query
    /// </summary>
    public sealed class PatchByQueryOperation : IOperation<OperationIdResult>
    {
        private readonly IndexQuery _queryToUpdate;
        private readonly QueryOperationOptions _options;

        /// <summary>
        ///     Returns an awaitable operation which updates all documents according to the provided <paramref name="queryToUpdate"/>
        /// </summary>
        /// <param name="queryToUpdate">The patch query according to which the documents will be updated</param>
        public PatchByQueryOperation(string queryToUpdate)
            : this(new IndexQuery { Query = queryToUpdate })
        {
        }

        /// <summary>
        ///     Returns an awaitable operation which updates all documents according to the provided <paramref name="queryToUpdate"/>
        /// </summary>
        /// <param name="queryToUpdate">An object containing the patch query according to which the documents will be updated, as well as optional parameters</param>
        /// <param name="options">Provides additional options for configuring the patch execution when updating documents</param>
        public PatchByQueryOperation(IndexQuery queryToUpdate, QueryOperationOptions options = null)
        {
            _queryToUpdate = queryToUpdate ?? throw new ArgumentNullException(nameof(queryToUpdate));
            _options = options;
        }

        public RavenCommand<OperationIdResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new PatchByQueryCommand<Parameters>(conventions, context, _queryToUpdate, _options);
        }

        internal sealed class PatchByQueryCommand<T> : RavenCommand<OperationIdResult>
        {
            private readonly DocumentConventions _conventions;
            private readonly IndexQuery<T> _queryToUpdate;
            private readonly long? _operationId;
            private readonly QueryOperationOptions _options;

            public PatchByQueryCommand(DocumentConventions conventions, JsonOperationContext context,
                IndexQuery<T> queryToUpdate,
                QueryOperationOptions options = null,
                long? operationId = null)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                _queryToUpdate = queryToUpdate ?? throw new ArgumentNullException(nameof(queryToUpdate));
                _operationId = operationId;
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

                if (_options.IgnoreMaxStepsForScript)
                {
                    path
                        .Append("&ignoreMaxStepsForScript=")
                        .Append(_options.IgnoreMaxStepsForScript);
                }

                if (_operationId.HasValue)
                {
                    path
                        .Append("&operationId=")
                        .Append(_operationId.Value);
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
                    }, _conventions)
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
