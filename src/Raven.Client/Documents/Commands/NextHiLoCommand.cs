using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Identity;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class NextHiLoCommand : RavenCommand<HiLoResult>
    {
        private readonly string _tag;
        private readonly long _lastBatchSize;
        private readonly DateTime _lastRangeAt;
        private readonly char _identityPartsSeparator;
        private readonly long _lastRangeMax;

        public NextHiLoCommand(string tag, long lastBatchSize, DateTime lastRangeAt, char identityPartsSeparator, long lastRangeMax)
        {
            _tag = tag ?? throw new ArgumentNullException(nameof(tag));
            _lastBatchSize = lastBatchSize;
            _lastRangeAt = lastRangeAt;
            _identityPartsSeparator = identityPartsSeparator;
            _lastRangeMax = lastRangeMax;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var pathBuilder = new StringBuilder();

            pathBuilder.Append(node.Url)
                    .Append("/databases/")
                    .Append(node.Database)
                    .Append("/hilo/next?tag=")
                    .Append(Uri.EscapeDataString(_tag))
                    .Append("&lastBatchSize=")
                    .Append(_lastBatchSize)
                    .Append("&lastRangeAt=")
                    .Append(_lastRangeAt.GetDefaultRavenFormat())
                    .Append("&identityPartsSeparator=")
                    .Append(Uri.EscapeDataString(_identityPartsSeparator.ToString()))
                    .Append("&lastMax=")
                    .Append(_lastRangeMax);

            url = pathBuilder.ToString();

            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            Result = JsonDeserializationClient.HiLoResult(response);
        }

        public override bool IsReadRequest => true;
    }
}
