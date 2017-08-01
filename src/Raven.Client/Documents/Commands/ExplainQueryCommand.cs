using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class ExplainQueryCommand : RavenCommand<ExplainQueryCommand.ExplainQueryResult[]>
    {
        public class ExplainQueryResult
        {
            public string Index { get; set; }
            public string Reason { get; set; }
        }

        private readonly DocumentConventions _conventions;
        private readonly JsonOperationContext _context;
        private readonly IndexQuery _indexQuery;

        public ExplainQueryCommand(DocumentConventions conventions, JsonOperationContext context, IndexQuery indexQuery)
        {
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _context = context ?? throw new ArgumentNullException(nameof(context));
            _indexQuery = indexQuery ?? throw new ArgumentNullException(nameof(indexQuery));
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            var path = new StringBuilder(node.Url)
                .Append("/databases/")
                .Append(node.Database)
                .Append("/queries?debug=explain");

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

            if (response.TryGet("Results", out BlittableJsonReaderArray array) == false)
            {
                ThrowInvalidResponse();
                return; // never hit
            }

            var results = new ExplainQueryResult[array.Length];
            for (var i = 0; i < array.Length; i++)
            {
                var result = (BlittableJsonReaderObject)array[i];
                results[i] = (ExplainQueryResult)_conventions.DeserializeEntityFromBlittable(typeof(ExplainQueryResult), result);
            }

            Result = results;
        }

        public override bool IsReadRequest => true;
    }
}