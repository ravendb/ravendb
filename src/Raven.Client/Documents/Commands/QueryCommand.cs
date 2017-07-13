using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class QueryCommand : RavenCommand<QueryResult>
    {
        private readonly DocumentConventions _conventions;
        private readonly JsonOperationContext _context;
        private readonly IndexQuery _indexQuery;
        private readonly bool _metadataOnly;
        private readonly bool _indexEntriesOnly;

        public QueryCommand(DocumentConventions conventions, JsonOperationContext context, IndexQuery indexQuery, bool metadataOnly = false, bool indexEntriesOnly = false)
        {
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _context = context ?? throw new ArgumentNullException(nameof(context));
            _indexQuery = indexQuery ?? throw new ArgumentNullException(nameof(indexQuery));
            _metadataOnly = metadataOnly;
            _indexEntriesOnly = indexEntriesOnly;

            if (indexQuery.WaitForNonStaleResultsTimeout.HasValue && indexQuery.WaitForNonStaleResultsTimeout != TimeSpan.MaxValue)
                Timeout = indexQuery.WaitForNonStaleResultsTimeout.Value.Add(TimeSpan.FromSeconds(10)); // giving the server an opportunity to finish the response
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            var path = new StringBuilder(node.Url)
                .Append("/databases/")
                .Append(node.Database)
                .Append("/queries");

            if (_metadataOnly)
                path.Append("?metadata-only=true");

            if (_indexEntriesOnly)
            {
                path.Append(_metadataOnly == false ? "?" : "&");
                path.Append("debug=entries");
            }

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(stream =>
                    {
                        using (var writer = new BlittableJsonTextWriter(_context, stream))
                        {
                            writer.WriteIndexQuery(_conventions, _context, _indexQuery);
                        }
                    }
                )
            };

            url = path.ToString();
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
            {
                Result = null;
                return;
            }

            Result = JsonDeserializationClient.QueryResult(response);

            if (fromCache)
                Result.DurationInMs = -1;
        }

        public override bool IsReadRequest => true;
    }
}