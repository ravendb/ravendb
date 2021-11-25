using System.Net.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Commands
{
    public class SubscriptionTryoutCommand : RavenCommand<GetDocumentsResult>
    {
        private readonly SubscriptionTryout _tryout;
        private readonly int _pageSize;
        private readonly int? _timeLimit;

        public SubscriptionTryoutCommand(SubscriptionTryout tryout, int pageSize, int? timeLimit)
        {
            _tryout = tryout;
            _pageSize = pageSize;
            _timeLimit = timeLimit;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/subscriptions/try?pageSize={_pageSize}";
            if (_timeLimit.HasValue)
            {
                url += $"&timeLimit={_timeLimit}";
            }

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                    {
                        writer.WriteStartObject();
                        writer.WritePropertyName(nameof(SubscriptionTryout.ChangeVector));
                        writer.WriteString(_tryout.ChangeVector);
                        writer.WritePropertyName(nameof(SubscriptionTryout.Query));
                        writer.WriteString(_tryout.Query);
                        writer.WriteEndObject();
                    }
                })
            };

            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
            {
                Result = null;
                return;
            }

            Result = JsonDeserializationClient.GetDocumentsResult(response);
        }

        public override bool IsReadRequest => true;
    }
}
