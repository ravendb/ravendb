using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Sparrow.Json;
using Raven.Abstractions;

namespace Raven.NewClient.Commands
{
    public class HiLoReturnCommand : RavenCommand<HiLoResult>
    {
        public string Tag;
        public long Last;
        public long End;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            var pathBuilder = new StringBuilder("hilo/return?");

            pathBuilder.Append($"tag={Tag}");
            pathBuilder.Append($"&end={End}");
            pathBuilder.Append($"&last={Last}");

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };

            url = $"{node.Url}/databases/{node.Database}/" + pathBuilder;
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response) { }

    }
}
