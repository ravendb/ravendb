using System;
using System.Net.Http;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class NextIdentityForCommand : RavenCommand<long>
    {
        private readonly string _id;

        public NextIdentityForCommand(string id)
        {
            _id = id ?? throw new ArgumentNullException(nameof(id));
        }

        public override bool IsReadRequest { get; } = false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            EnsureIsNotNullOrEmpty(_id, nameof(_id));

            url = $"{node.Url}/databases/{node.Database}/identity/next?name={UrlEncode(_id)}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post
            };
            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null || response.TryGet("NewIdentityValue", out long results) == false)
            {
                ThrowInvalidResponse();
                return; // never hit
            }


            Result = results;
        }
    }
}
