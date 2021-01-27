using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding;
using Sparrow.Json;

namespace Raven.Server.Documents.ShardedHandlers.ShardedCommands
{
    internal class ShardedHeadCommand : ShardedBaseCommand<string>
    {
        public override Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
        {
            if (response.StatusCode == HttpStatusCode.NotModified)
            {
                Result = Headers["If-None-Match"];
                return Task.FromResult(ResponseDisposeHandling.Automatic);
            }

            if (response.StatusCode == HttpStatusCode.NotFound)
            {
                Result = null;
                return Task.FromResult(ResponseDisposeHandling.Automatic);
            }

            Result = response.GetRequiredEtagHeader();
            return Task.FromResult(ResponseDisposeHandling.Automatic);
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            Debug.Assert(fromCache == false);

            if (response != null)
                ThrowInvalidResponse();

            Result = null;
        }

        public ShardedHeadCommand(ShardedRequestHandler handler, Headers headers, BlittableJsonReaderObject content = null) : base(handler, headers, content)
        {
        }
    }
}
