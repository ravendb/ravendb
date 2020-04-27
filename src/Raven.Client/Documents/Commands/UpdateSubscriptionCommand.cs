using System.Net.Http;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    internal class UpdateSubscriptionCommand : RavenCommand<UpdateSubscriptionResult>, IRaftCommand
    {
        private readonly SubscriptionUpdateOptions _options;

        public UpdateSubscriptionCommand(SubscriptionUpdateOptions options)
        {
            _options = options;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/subscriptions/update";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(stream =>
                {
                    ctx.Write(stream, 
                        EntityToBlittable.ConvertCommandToBlittable(_options, ctx));
                })
            };
            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (fromCache)
            {
                Result = new UpdateSubscriptionResult
                {
                    Name = _options.Name
                };
                return;
            }

            if (response == null)
                ThrowInvalidResponse();

            Result = JsonDeserializationClient.UpdateSubscriptionResult(response);
        }

        public override bool IsReadRequest => false;
        public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
    }
}
