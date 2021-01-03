using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands;
using Raven.Server.Documents.Sharding;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class ShardedDocumentHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/docs", "HEAD")]
        public async Task Head()
        {
            var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
            var changeVector = GetStringFromHeaders("If-None-Match");

            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var index = ShardedContext.GetShardIndex(context, id);

                var cmd = new ShardedHeadCommand
                {
                    Id = id,
                    ChangeVector = changeVector
                    
                };

                await ShardedContext.RequestExecutors[index].ExecuteAsync(cmd, context);
                HttpContext.Response.StatusCode = (int)cmd.StatusCode;
                HttpContext.Response.Headers[Constants.Headers.Etag] = cmd.Result;
            }
        }

        [RavenShardedAction("/databases/*/docs", "PUT")]
        public async Task Put()
        {
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
                var doc = await context.ReadForDiskAsync(RequestBodyStream(), id).ConfigureAwait(false);

                if (id[^1] == '|')
                {
                    // note that we use the _overall_ database for this, not the specific shards
                    var (_, clusterId, _) = await ServerStore.GenerateClusterIdentityAsync(id, '/', ShardedContext.DatabaseName, GetRaftRequestIdFromQuery());
                    id = clusterId;
                }
                var changeVector = context.GetLazyString(GetStringFromHeaders("If-Match"));

                var index = ShardedContext.GetShardIndex(context, id);

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

                HttpContext.Response.Headers[Constants.Headers.Etag] = cmd.Response?.Headers?.ETag?.Tag;

                cmd.Result.WriteJsonTo(ResponseBodyStream());
            }
        }

        [RavenShardedAction("/databases/*/docs", "PATCH")]
        public async Task Patch()
        {
            var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
            var isTest = GetBoolValueQueryString("test", required: false) ?? false;
            var debugMode = GetBoolValueQueryString("debug", required: false) ?? isTest;
            var skipPatchIfChangeVectorMismatch = GetBoolValueQueryString("skipPatchIfChangeVectorMismatch", required: false) ?? false;

            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var changeVector = context.GetLazyString(GetStringFromHeaders("If-Match"));

                var patch = context.Read(RequestBodyStream(), "ScriptedPatchRequest");

                var index = ShardedContext.GetShardIndex(context, id);

                var cmd = new ShardedCommand
                {
                    Method = HttpMethod.Patch,
                    Url = $"/docs{HttpContext.Request.QueryString.ToString()}",
                    Content = patch,
                    Headers =
                    {
                        ["If-Match"] = changeVector
                    }
                };

                await ShardedContext.RequestExecutors[index].ExecuteAsync(cmd, context);
                HttpContext.Response.StatusCode = (int)cmd.StatusCode;
                cmd.Result.WriteJsonTo(ResponseBodyStream());
            }
        }

        [RavenShardedAction("/databases/*/docs", "GET")]
        public async Task Get()
        {
            var ids = GetStringValuesQueryString("id", required: false);
            var metadataOnly = GetBoolValueQueryString("metadataOnly", required: false) ?? false;
            var includePaths = GetStringValuesQueryString("include", required: false);
            var etag = GetStringFromHeaders("If-None-Match");

            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                await GetDocumentsAsync(ids, includePaths, etag, metadataOnly, context);
            }
        }

        private async Task GetDocumentsAsync(StringValues ids, StringValues includePaths, string etag, bool metadataOnly, TransactionOperationContext context)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            var sb = new StringBuilder();
            var cmds = new List<ShardedCommand>();
            List<ShardedCommand> oldCmds = null;

            var tasks = new List<Task>();
            try
            {
                await FetchDocumentsFromShards(ids, metadataOnly, context, sb, includePaths, cmds, tasks);

                if (cmds.Count == 1 && includePaths.Any() == false)
                {
                    var singleEtag = cmds[0].Response?.Headers?.ETag?.Tag;
                    if (etag == singleEtag)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                        return;
                    }
                    //TODO: verify all headers are taken care of
                    HttpContext.Response.StatusCode = (int)cmds[0].StatusCode;
                    HttpContext.Response.Headers[Constants.Headers.Etag] = singleEtag;
                    cmds[0].Result?.WriteJsonTo(ResponseBodyStream());
                    return;
                }

                // here we know that all of them are good
                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

                var actualEtag = ComputeHttpEtags.CombineEtags(EnumerateEtags(cmds));
                if (etag == actualEtag)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return;
                }

                HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + actualEtag + "\"";


                var results = new BlittableJsonReaderObject[ids.Count];
                var includesMap = new Dictionary<string, BlittableJsonReaderObject>(StringComparer.OrdinalIgnoreCase);
                var missingIncludes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var cmd in cmds)
                {
                    if (cmd.Response == null)
                        continue;
                    cmd.Result.TryGet(nameof(GetDocumentsResult.Results), out BlittableJsonReaderArray cmdResults);
                    if (cmd.Result.TryGet(nameof(GetDocumentsResult.Includes), out BlittableJsonReaderObject cmdIncludes))
                    {
                        BlittableJsonReaderObject.PropertyDetails prop = default;
                        for (int i = 0; i < cmdIncludes.Count; i++)
                        {
                            cmdIncludes.GetPropertyByIndex(i, ref prop);
                            includesMap[prop.Name] = (BlittableJsonReaderObject)prop.Value;
                        }
                    }

                    for (var index = 0; index < cmd.PositionMatches.Count; index++)
                    {
                        int match = cmd.PositionMatches[index];
                        var result = (BlittableJsonReaderObject)cmdResults[index];
                        foreach (string includePath in includePaths)
                        {
                            IncludeUtil.GetDocIdFromInclude(result, includePath, missingIncludes, '/');
                        }

                        results[match] = result;
                    }
                }

                foreach (var kvp in includesMap) // remove the items we already have
                {
                    missingIncludes.Remove(kvp.Key);
                }

                if (missingIncludes.Count > 0)
                {
                    oldCmds = cmds; //fetch missing includes will override the cmds and we can't dispose of them yet
                    await FetchMissingIncludes(cmds, tasks, missingIncludes, context, sb, includePaths, includesMap, metadataOnly);
                }

                using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream(), ServerStore.ServerShutdown))
                {
                    writer.WriteStartObject();

                    await writer.WriteArrayAsync(nameof(GetDocumentsResult.Results), results);

                    writer.WriteComma();

                    await WriteIncludesAsync(includesMap, writer);

                    writer.WriteEndObject();
                    await writer.OuterFlushAsync();
                    //TODO: Add performance hints
                }
            
            }
            finally
            {
                if (oldCmds != null)
                {
                    foreach (ShardedCommand cmd in oldCmds)
                    {
                        cmd.Disposable.Dispose();
                    }
                }

                foreach (ShardedCommand cmd in cmds)
                {
                    cmd.Disposable.Dispose();
                }
            }
        }

        [RavenShardedAction("/databases/*/docs", "POST")]
        public async Task PostGet()
        {
            var metadataOnly = GetBoolValueQueryString("metadataOnly", required: false) ?? false;
            var includePaths = GetStringValuesQueryString("include", required: false);
            var etag = GetStringFromHeaders("If-None-Match");

            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var docs = await context.ReadForMemoryAsync(RequestBodyStream(), "docs");
                if (docs.TryGet("Ids", out BlittableJsonReaderArray array) == false)
                    ThrowRequiredPropertyNameInRequest("Ids");

                var ids = new string[array.Length];
                for (int i = 0; i < array.Length; i++)
                {
                    ids[i] = array.GetStringByIndex(i);
                }

                var idsStringValues = new Microsoft.Extensions.Primitives.StringValues(ids);

                if (TrafficWatchManager.HasRegisteredClients)
                    HttpContext.Items["TrafficWatch"] = (idsStringValues.ToString(), TrafficWatchChangeType.Documents);

                await GetDocumentsAsync(ids, includePaths, etag, metadataOnly, context);
            }
        }
        private async Task WriteIncludesAsync(Dictionary<string, BlittableJsonReaderObject> includesMap, AsyncBlittableJsonTextWriter writer)
        {
            writer.WritePropertyName(nameof(GetDocumentsResult.Includes));
            writer.WriteStartObject();
            var first = true;
            foreach (var kvp in includesMap)
            {
                if (first == false)
                    writer.WriteComma();
                first = false;

                writer.WritePropertyName(kvp.Key);
                if (kvp.Value != null)
                {
                    writer.WriteObject(kvp.Value);
                }
                else
                {
                    writer.WriteNull();
                }
                await writer.MaybeOuterFlushAsync();
            }
            writer.WriteEndObject();
        }

        private async Task FetchMissingIncludes(List<ShardedCommand> cmds, List<Task> tasks, HashSet<string> missingIncludes, TransactionOperationContext context, StringBuilder sb, StringValues includePaths,
            Dictionary<string, BlittableJsonReaderObject> includesMap, bool metadataOnly)
        {
            cmds = new List<ShardedCommand>();
            tasks.Clear();
            var missingList = missingIncludes.ToList();
            await FetchDocumentsFromShards(missingList, metadataOnly, context, sb.Clear(), includePaths, cmds, tasks, ignoreIncludes: true);
            foreach (var cmd in cmds)
            {
                if (cmd.Response == null)
                    continue;

                cmd.Result.TryGet(nameof(GetDocumentsResult.Results), out BlittableJsonReaderArray cmdResults);
                for (var index = 0; index < cmd.PositionMatches.Count; index++)
                {
                    var result = (BlittableJsonReaderObject)cmdResults[index];
                    includesMap.Add(missingList[cmd.PositionMatches[index]], result);
                }
            }
        }

        private async Task FetchDocumentsFromShards(IList<string> ids,bool metadataOnly, TransactionOperationContext context, StringBuilder sb, StringValues includePaths,
            List<ShardedCommand> cmds, List<Task> tasks, bool ignoreIncludes = false)
        {
            var shards = ShardLocator.GetDocumentIdsShards(ids, ShardedContext, context);
            foreach (var shard in shards)
            {
                sb.Clear();
                sb.Append("/docs?");
                if(ignoreIncludes == false)
                {
                    foreach (string includePath in includePaths)
                    {
                        sb.Append("&include=").Append(Uri.EscapeDataString(includePath));
                    }
                }

                if (metadataOnly)
                {
                    sb.Append("&metadataOnly=true");
                }

                var matches = new List<int>();
                foreach (var idIdx in shard.Value)
                {
                    sb.Append($"&id={Uri.EscapeUriString(ids[idIdx])}");
                    matches.Add(idIdx);
                }

                var cmd = new ShardedCommand
                {
                    Method = HttpMethod.Get,
                    Url = sb.ToString(),
                    PositionMatches = matches,
                    Disposable = ContextPool.AllocateOperationContext(out TransactionOperationContext ctx)
                };
                cmds.Add(cmd);
                var task = ShardedContext.RequestExecutors[shard.Key].ExecuteAsync(cmd, ctx);
                tasks.Add(task);
            }

            await Task.WhenAll(tasks); // if any failed, we explicitly let it throw here
        }

        private IEnumerable<string> EnumerateEtags(List<ShardedCommand> cmds)
        {
            foreach (var cmd in cmds)
            {
                string etag = cmd.Response?.Headers?.ETag?.Tag;
                if (etag != null)
                    yield return etag;
            }
        }
    }
}
