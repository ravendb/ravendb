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
    public class NextHiLoCommand : RavenCommand<HiLoResult>
    {
        public string Tag;
        public long LastBatchSize;
        public DateTime LastRangeAt;
        public string IdentityPartsSeparator;
        public long LastRangeMax;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {            
            var pathBuilder = new StringBuilder("hilo/next?");

            pathBuilder.Append($"tag={Tag}");

            if (LastBatchSize != null)
                pathBuilder.Append($"&lastBatchSize={LastBatchSize}");

            if (LastRangeAt != null)
                pathBuilder.Append($"&lastRangeAt={LastRangeAt.ToString("o")}");
            
            if (string.IsNullOrEmpty(IdentityPartsSeparator) == false)
                pathBuilder.Append($"&identityPartsSeparator={IdentityPartsSeparator}");

            if (LastRangeAt != null)
                pathBuilder.Append($"&lastMax={LastRangeMax}");

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };
            
            url = $"{node.Url}/databases/{node.Database}/" + pathBuilder;
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response)
        {
            Result = JsonDeserializationClient.HiLoResult(response);
        }
    }
}
