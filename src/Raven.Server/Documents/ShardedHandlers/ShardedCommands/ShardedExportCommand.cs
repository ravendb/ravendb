using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Documents.ShardedHandlers.ShardedCommands
{
    public class ShardedExportCommand : ShardedStreamCommand
    {
        private readonly long _operationId;
        private readonly DatabaseSmugglerOptionsServerSide _options;

        public ShardedExportCommand(ShardedRequestHandler handler, long operationId, DatabaseSmugglerOptionsServerSide options, Func<Stream, Task> handleStreamResponse) : 
            base(handler, handleStreamResponse, content: null)
        {
            _operationId = operationId;
            _options = options;

            var queryString = HttpUtility.ParseQueryString(handler.HttpContext.Request.QueryString.Value);
            queryString["operationId"] = _operationId.ToString();
            Url = handler.BaseShardUrl + "?" + queryString;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            Content = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_options, ctx);
            return base.CreateRequest(ctx, node, out url);
        }
    }
}
