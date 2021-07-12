using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class CreateSubscriptionCommand : RavenCommand<CreateSubscriptionResult>, IRaftCommand
    {
        private readonly SubscriptionCreationOptions _options;
        private readonly string _id;
        private readonly bool _disabled;

        public CreateSubscriptionCommand(DocumentConventions conventions, SubscriptionCreationOptions options, string id = null)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _id = id;
            _disabled = false;
        }
        public CreateSubscriptionCommand(DocumentConventions conventions, SubscriptionCreationOptions options, bool disabled, string id = null)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _id = id;
            _disabled = disabled;
        }
       

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/subscriptions";
            url += "?disabled=" + Uri.EscapeDataString(_disabled.ToString());
            if (_id != null)
                url += "&id=" + Uri.EscapeDataString(_id);
            
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Put,
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
            Result = JsonDeserializationClient.CreateSubscriptionResult(response);
        }

        public override bool IsReadRequest => false;
        public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
    }
}
