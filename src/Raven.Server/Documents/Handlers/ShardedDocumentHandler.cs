using System;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Commands.Batches;
using Raven.Server.Extensions;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers
{
    public class ShardedDocumentHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/docs", "GET")]
        public async Task Get()
        {
            var ids = GetStringValuesQueryString("id", required: false);
            var metadataOnly = GetBoolValueQueryString("metadataOnly", required: false) ?? false;
            var includePaths = GetStringValuesQueryString("include", required: false);
            var etag = GetStringFromHeaders("If-None-Match");

            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;

            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {

                var id = ids.Single(); // TODO: make it work with multiple shards

                var shardId = ShardedContext.GetShardId(context, id);

                var index = ShardedContext.GetShardIndex(shardId);

                var cmd = new ShardedCommand
                {
                    Method = HttpMethod.Get,
                    Url = $"/docs?id={Uri.EscapeUriString(id)}",
                };
                await ShardedContext.RequestExecutors[index].ExecuteAsync(cmd, context);
                string responseEtag = cmd.Response?.Headers?.ETag?.Tag;
                if (responseEtag != null)
                    HttpContext.Response.Headers.Add(Constants.Headers.Etag, responseEtag);

                HttpContext.Response.StatusCode = (int)cmd.StatusCode;
                //TODO: Pass the ETag
                cmd.Result?.WriteJsonTo(ResponseBodyStream());
            }
        }

        [RavenShardedAction("/databases/*/docs", "PUT")]
        public async Task Put()
        {
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
                var doc = await context.ReadForDiskAsync(RequestBodyStream(), id).ConfigureAwait(false);

                if (id[id.Length - 1] == '|')
                {
                    // note that we use the _overall_ database for this, not the specific shards
                    var (_, clusterId, _) = await ServerStore.GenerateClusterIdentityAsync(id, ShardedContext.DatabaseName, GetRaftRequestIdFromQuery());
                    id = clusterId;
                }
                var changeVector = context.GetLazyString(GetStringFromHeaders("If-Match"));

                var shardId = ShardedContext.GetShardId(context, id);

                var index = ShardedContext.GetShardIndex(shardId);

                var cmd = new ShardedCommand
                {
                    Method = HttpMethod.Put,
                    Url = $"/docs?id={Uri.EscapeUriString(id)}",
                    Content = doc,
                    Headers =
                    {
                        ["If-Match"] = changeVector
                    }
                };
                await ShardedContext.RequestExecutors[index].ExecuteAsync(cmd, context);
                HttpContext.Response.StatusCode = (int)cmd.StatusCode;
                //TODO: Pass the ETag
                cmd.Result.WriteJsonTo(ResponseBodyStream());
            }
        }
    }
}
