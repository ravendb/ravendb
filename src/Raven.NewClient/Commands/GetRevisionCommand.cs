using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Extensions;
using Raven.NewClient.Client.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.NewClient.Client.Http;

namespace Raven.NewClient.Client.Commands
{
    public class GetRevisionCommand : RavenCommand<BlittableArrayResult>
    {

        public string Key;
        public int Start;
        public int PageSize;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };

            url = $"{node.Url}/databases/{node.Database}/revisions?id={Uri.EscapeDataString(Key)}&start={Start.ToInvariantString()}&pageSize={PageSize.ToInvariantString()}";
            return request;
        }


        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                throw new InvalidOperationException();
            Result = JsonDeserializationClient.BlittableArrayResult(response);
        }

        public override bool IsReadRequest => true;
    }
}