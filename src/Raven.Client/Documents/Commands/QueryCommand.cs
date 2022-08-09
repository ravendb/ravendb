using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public abstract class AbstractQueryCommand<TParameters, TResult> : RavenCommand<TResult>
    {
        private static readonly TimeSpan AdditionalTimeToAddToTimeout = TimeSpan.FromSeconds(10);

        private readonly bool _metadataOnly;
        private readonly bool _indexEntriesOnly;

        protected AbstractQueryCommand(IndexQueryBase<TParameters> indexQuery, bool canCache, bool metadataOnly, bool indexEntriesOnly)
        {
            _metadataOnly = metadataOnly;
            _indexEntriesOnly = indexEntriesOnly;

            if (indexQuery.WaitForNonStaleResultsTimeout.HasValue && indexQuery.WaitForNonStaleResultsTimeout != TimeSpan.MaxValue)
            {
                var timeout = indexQuery.WaitForNonStaleResultsTimeout.Value;
                if (timeout < RequestExecutor.GlobalHttpClientTimeout) // if it is greater than it will throw in RequestExecutor
                {
                    timeout = RequestExecutor.GlobalHttpClientTimeout - timeout > AdditionalTimeToAddToTimeout
                        ? timeout.Add(AdditionalTimeToAddToTimeout) : RequestExecutor.GlobalHttpClientTimeout; // giving the server an opportunity to finish the response
                }

                Timeout = timeout;
            }

            CanCache = canCache;

            // we won't allow aggressive caching of queries with WaitForNonStaleResults
            CanCacheAggressively = CanCache && indexQuery.WaitForNonStaleResults == false;
        }

        public override bool IsReadRequest => true;

        protected abstract ulong GetQueryHash(JsonOperationContext ctx);

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var path = new StringBuilder(node.Url)
                .Append("/databases/")
                .Append(node.Database)
                .Append("/queries?queryHash=")
                // we need to add a query hash because we are using POST queries
                // so we need to unique parameter per query so the query cache will
                // work properly
                .Append(GetQueryHash(ctx));

            if (_metadataOnly)
                path.Append("&metadataOnly=true");

            if (_indexEntriesOnly)
            {
                path.Append("&debug=entries");
            }

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = GetContent(ctx)
            };

            url = path.ToString();
            return request;
        }

        protected abstract HttpContent GetContent(JsonOperationContext ctx);
    }

    public class QueryCommand : AbstractQueryCommand<Parameters, QueryResult>
    {
        private readonly DocumentConventions _conventions;
        private readonly IndexQuery _indexQuery;
        private readonly InMemoryDocumentSessionOperations _session;

        public QueryCommand(InMemoryDocumentSessionOperations session, IndexQuery indexQuery, bool metadataOnly = false, bool indexEntriesOnly = false) : base(indexQuery, indexQuery.DisableCaching == false, metadataOnly, indexEntriesOnly)
        {
            _indexQuery = indexQuery ?? throw new ArgumentNullException(nameof(indexQuery));
            _session = session ?? throw new ArgumentNullException(nameof(session));
            _conventions = _session.Conventions ?? throw new ArgumentNullException(nameof(_session.Conventions));
        }

        protected override ulong GetQueryHash(JsonOperationContext ctx)
        {
            return _indexQuery.GetQueryHash(ctx, _conventions, _session.JsonSerializer);
        }

        protected override HttpContent GetContent(JsonOperationContext ctx)
        {
            return new BlittableJsonContent(async stream =>
                {
                    // this is here to catch people closing the session before the ToListAsync() completes
                    _session.AssertNotDisposed();
                    await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                    {
                        writer.WriteIndexQuery(_conventions, ctx, _indexQuery);
                    }
                }
            );
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
            {
                Result = null;
                return;
            }
            _session.AssertNotDisposed(); // verify that we don't have async query with the user closing the session
            if (fromCache)
            {
                // we have to clone the response here because  otherwise the cached item might be freed while
                // we are still looking at this result, so we clone it to the side
                response = response.Clone(context);
            }
            Result = JsonDeserializationClient.QueryResult(response);

            if (fromCache)
            {
                Result.DurationInMs = -1;
                if (Result.Timings != null)
                {
                    Result.Timings.DurationInMs = -1;
                    Result.Timings = null;
                }
            }
        }
    }
}
