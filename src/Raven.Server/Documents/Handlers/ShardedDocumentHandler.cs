using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Commands.Batches;
using Raven.Server.Documents.Sharding;
using Raven.Server.Extensions;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class ShardedDocumentHandler : ShardedRequestHandler
    {
        protected readonly StringBuilder UrlStringBuilder = new StringBuilder();
        protected readonly List<Task> Task = new List<Task>();
        protected readonly Dictionary<int, ShardedCommand> Cmds = new Dictionary<int, ShardedCommand>();
        protected readonly List<TransactionOperationContext> CmdsContext = new List<TransactionOperationContext>();

        [RavenShardedAction("/databases/*/docs", "GET")]
        public async Task Get()
        {
            var ids = GetStringValuesQueryString("id", required: false);
            var metadataOnly = GetBoolValueQueryString("metadataOnly", required: false) ?? false;
            var includePaths = GetStringValuesQueryString("include", required: false);
            var etag = GetStringFromHeaders("If-None-Match");

            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;

            try
            {
                using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {

                    var shards = ShardLocator.GetDocumentIdsShards(ids, ShardedContext, context);
                    foreach (var shard in shards.ShardsToIds)
                    {

                        UrlStringBuilder.Clear();
                        UrlStringBuilder.Append("/docs?");
                        foreach (var id in shard.Value)
                        {
                            UrlStringBuilder.Append($"&id={Uri.EscapeUriString(id)}");
                        }

                        var cmd = new ShardedCommand {Method = HttpMethod.Get, Url = UrlStringBuilder.ToString()};
                        ContextPool.AllocateOperationContext(out TransactionOperationContext ctx);
                        CmdsContext.Add(ctx);

                        var task = ShardedContext.RequestExecutors[shard.Key].ExecuteAsync(cmd, ctx);
                        Task.Add(task);
                        Cmds.Add(shard.Key, cmd);
                    }

                    await System.Threading.Tasks.Task.WhenAll(Task);

                    HttpContext.Response.StatusCode = (int)ShardedStatusCodeChooser.GetStatusCode(Cmds.Values);

                    if (HttpContext.Response.IsSuccessStatusCode() == false)
                        return;

                    var actualEtag = ComputeHttpEtags.CombineEtags(EnumerateEtags(Cmds));

                    if (etag == actualEtag)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                        return;
                    }

                    HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + actualEtag + "\"";

                    using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream(), ServerStore.ServerShutdown))
                    {
                        writer.WriteStartObject();

                        WriteArray(writer, nameof(GetDocumentsResult.Results), ids, shards);

                        //TODO: write includes

                        writer.WriteEndObject();
                        await writer.OuterFlushAsync();
                        //TODO: Add performance hints
                    }
                }
            }
            finally
            {
                Task.Clear();
                Cmds.Clear();

                Parallel.ForEach(CmdsContext, c =>
                {
                    c.Dispose();
                });
            }
        }

        private IEnumerable<string> EnumerateEtags(Dictionary<int, ShardedCommand> cmds)
        {
            foreach (var cmd in cmds.Values)
            {
                yield return cmd.Response?.Headers?.ETag?.Tag;
            }
        }

        private int WriteArray(AsyncBlittableJsonTextWriter writer, string arrayName, IEnumerable<string> ids,
            ShardLocator.ShardLocatorResults shardPositions )
        {
            int numberOfItems = 0;
            writer.WritePropertyName(arrayName);

            writer.WriteStartArray();

            bool first = true;
            Dictionary<int, BlittableJsonReaderArray> results = new Dictionary<int, BlittableJsonReaderArray>();

            foreach (var cmd in Cmds)
            {
                if (cmd.Value.Result.TryGet<BlittableJsonReaderArray>(arrayName, out var result) == false)
                {
                    //This should not happen
                    Debug.Assert(false, $"Missing results for shard #{cmd.Value}");
                    continue;
                }
                results.Add(cmd.Key, result);
            }

            //We must keep the same order of ids
            foreach (var id in ids)
            {
                if (shardPositions.IdsToShardPosition.TryGetValue(id, out var pos) == false)
                {
                    //This should not happen
                    Debug.Assert(false, "Got an id that had no shard position");
                    continue; 
                }

                var document = results[pos.ShardId].GetByIndex<BlittableJsonReaderObject>(pos.Position);

                if (first == false)
                {
                    writer.WriteComma();
                }
                first = false;

                writer.WriteObject(document);
                numberOfItems++;
            }

            writer.WriteEndArray();
            return numberOfItems;
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
