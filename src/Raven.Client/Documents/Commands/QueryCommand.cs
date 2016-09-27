using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Data;
using Raven.Client.Data.Queries;
using Raven.Client.Document;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class QueryCommand : RavenCommand<QueryResult>
    {
        public string index;
        public IndexQuery indexQuery;
        public DocumentConvention _convention;
        public string[] includes;
        public bool MetadataOnly;
        public bool IndexEntriesOnly;

        public JsonOperationContext Context;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            var method = (indexQuery.Query == null || indexQuery.Query.Length <= _convention.MaxLengthOfQueryUsingGetUrl)
                ? HttpMethod.Get : HttpMethod.Post;

            var request = new HttpRequestMessage
            {
                Method = method
            };

            // TODO Iftah, if query string is too long, need to send as post.
            var indexQueryUrl = indexQuery.GetIndexQueryUrl(index, "queries", includeQuery: method == HttpMethod.Get);

            EnsureIsNotNullOrEmpty(indexQueryUrl, "index");

            var pathBuilder = new StringBuilder(indexQueryUrl);

            if (MetadataOnly)
                pathBuilder.Append("&metadata-only=true");
            if (IndexEntriesOnly)
                pathBuilder.Append("&debug=entries");
            if (includes != null && includes.Length > 0)
            {
                pathBuilder.Append("&").Append(string.Join("&", includes.Select(x => "include=" + x).ToArray()));
            }

            url = pathBuilder.ToString();
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response)
        {
            if (response == null)
            {
                Result = null;
                return;
            }
            
            Result = JsonDeserializationClient.QueryResult(response);
        }
    }
}