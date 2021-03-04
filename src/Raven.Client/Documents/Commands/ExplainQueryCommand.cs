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
        private readonly IndexQuery _indexQuery;

        public ExplainQueryCommand(DocumentConventions conventions, IndexQuery indexQuery)
        {
            _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
            _indexQuery = indexQuery ?? throw new ArgumentNullException(nameof(indexQuery));
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var path = new StringBuilder(node.Url)
                .Append("/databases/")
                .Append(node.Database)
                .Append("/queries?debug=explain");

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteIndexQuery(_conventions, ctx, _indexQuery);
                        }
                    }
                )
            };

            url = path.ToString();
            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
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
                results[i] = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<ExplainQueryResult>(result);
            }

            Result = results;
        }

        public override bool IsReadRequest => true;
    }
}
