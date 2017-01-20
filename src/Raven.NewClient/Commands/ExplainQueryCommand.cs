using System;
using System.Net.Http;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Sparrow.Json;

namespace Raven.NewClient.Client.Commands
{
    public class ExplainQueryCommand : RavenCommand<ExplainQueryCommand.ExplainQueryResult[]>
    {
        public class ExplainQueryResult
        {
            public string Index { get; set; }
            public string Reason { get; set; }
        }

        private readonly DocumentConvention _conventions;
        private readonly JsonOperationContext _context;
        private readonly string _indexName;
        private readonly IndexQuery _indexQuery;

        public ExplainQueryCommand(DocumentConvention conventions, JsonOperationContext context, string indexName, IndexQuery indexQuery)
        {
            if (conventions == null)
                throw new ArgumentNullException(nameof(conventions));
            if (context == null)
                throw new ArgumentNullException(nameof(context));
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));
            if (indexQuery == null)
                throw new ArgumentNullException(nameof(indexQuery));

            _conventions = conventions;
            _context = context;
            _indexName = indexName;
            _indexQuery = indexQuery;

            ResponseType = RavenCommandResponseType.Array;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            var method = _indexQuery.Query == null || _indexQuery.Query.Length <= _conventions.MaxLengthOfQueryUsingGetUrl
                ? HttpMethod.Get
                : HttpMethod.Post;

            var request = new HttpRequestMessage
            {
                Method = method
            };

            if (method == HttpMethod.Post)
            {
                request.Content = new BlittableJsonContent(stream =>
                {
                    using (var writer = new BlittableJsonTextWriter(_context, stream))
                    {
                        writer.WriteStartObject();
                        writer.WritePropertyName("Query");
                        writer.WriteString(_indexQuery.Query);
                        writer.WriteEndObject();
                    }
                });
            }

            var indexQueryUrl = $"{_indexQuery.GetIndexQueryUrl(_indexName, "queries", includeQuery: method == HttpMethod.Get)}&debug=explain";

            url = $"{node.Url}/databases/{node.Database}/" + indexQueryUrl;
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response)
        {
            ThrowInvalidResponse();
        }

        public override void SetResponse(BlittableJsonReaderArray response)
        {
            if (response == null)
            {
                Result = null;
                return;
            }

            var results = new ExplainQueryResult[response.Length];
            for (var i = 0; i < response.Length; i++)
            {
                var result = (BlittableJsonReaderObject)response[i];
                results[i] = (ExplainQueryResult)_conventions.DeserializeEntityFromBlittable(typeof(ExplainQueryResult), result);
            }

            Result = results;
        }

        public override bool IsReadRequest => true;
    }
}