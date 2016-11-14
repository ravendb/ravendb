using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Sparrow.Json;

namespace Raven.NewClient.Client.Documents.Commands
{
    public class QueryCommand : RavenCommand<QueryResult>
    {
        public string Index;
        public IndexQuery IndexQuery;
        public DocumentConvention Convention;
        public HashSet<string> Includes;
        public bool MetadataOnly;
        public bool IndexEntriesOnly;
        public JsonOperationContext Context;
        
        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            var method = (IndexQuery.Query == null || IndexQuery.Query.Length <= Convention.MaxLengthOfQueryUsingGetUrl)
                ? HttpMethod.Get : HttpMethod.Post;

            var request = new HttpRequestMessage
            {
                Method = method
            };
            
            if (method == HttpMethod.Post)
            {
                request.Content = new BlittableJsonContent(stream =>
                {
                    using (var writer = new BlittableJsonTextWriter(Context, stream))
                    {
                        writer.WriteStartObject();
                        writer.WritePropertyName("Query");
                        writer.WriteString(IndexQuery.Query);
                        writer.WriteEndObject();
                    }
                });
            }

            var indexQueryUrl = IndexQuery.GetIndexQueryUrl(Index, "queries", includeQuery: method == HttpMethod.Get);

            EnsureIsNotNullOrEmpty(indexQueryUrl, "index");

            var pathBuilder = new StringBuilder(indexQueryUrl);

            if (MetadataOnly)
                pathBuilder.Append("&metadata-only=true");
            if (IndexEntriesOnly)
                pathBuilder.Append("&debug=entries");
            if (Includes != null && Includes.Count > 0)
            {
                pathBuilder.Append("&").Append(string.Join("&", Includes.Select(x => "include=" + x).ToArray()));
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