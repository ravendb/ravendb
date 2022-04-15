using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands;
using Raven.Client.Extensions;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.Documents.Sharding.Handlers.Processors.Documents;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedDocumentHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/docs", "HEAD")]
        public async Task Head()
        {
            using (var processor = new ShardedDocumentHandlerProcessorForHead(this, ContextPool))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenShardedAction("/databases/*/docs/size", "GET")]
        public async Task GetDocSize()
        {
            using (var processor = new ShardedDocumentHandlerProcessorForGetDocSize(this, ContextPool))
            {
                await processor.ExecuteAsync();
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
                    var (_, clusterId, _) = await ServerStore.GenerateClusterIdentityAsync(id, DatabaseContext.IdentityPartsSeparator, DatabaseContext.DatabaseName, GetRaftRequestIdFromQuery());
                    id = clusterId;
                }
                
                var index = DatabaseContext.GetShardNumber(context, id);
                var cmd = new ShardedCommand(this, Headers.IfMatch, content: doc);
                await DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(context, cmd, index);
                HttpContext.Response.StatusCode = (int)cmd.StatusCode;

                HttpContext.Response.Headers[Constants.Headers.Etag] = cmd.Response?.Headers?.ETag?.Tag;

                await cmd.Result.WriteJsonToAsync(ResponseBodyStream());
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
                var patch = await context.ReadForMemoryAsync(RequestBodyStream(), "ScriptedPatchRequest");

                var index = DatabaseContext.GetShardNumber(context, id);

                var cmd = new ShardedCommand(this, Headers.IfMatch, content: patch);

                await DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(context, cmd, index);
                HttpContext.Response.StatusCode = (int)cmd.StatusCode;
                await cmd.Result.WriteJsonToAsync(ResponseBodyStream());
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
                if (TrafficWatchManager.HasRegisteredClients) //TODO - sharding: do we need that here?
                    AddStringToHttpContext(ids.ToString(), TrafficWatchChangeType.Documents);

                if (ids.Count > 0)
                {
                    await GetDocumentsByIdAsync(ids, includePaths, etag, metadataOnly, context);
                }
                else
                {
                    await GetDocumentsAsync(context, metadataOnly);
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

                var idsStringValues = new StringValues(ids);

                if (TrafficWatchManager.HasRegisteredClients) //TODO - sharding: do we need that here?
                    AddStringToHttpContext(idsStringValues.ToString(), TrafficWatchChangeType.Documents);

                if (ids.Length > 0)
                {
                    await GetDocumentsByIdAsync(ids, includePaths, etag, metadataOnly, context);
                }
                else
                {
                    await GetDocumentsAsync(context, metadataOnly);
                }
            }
        }

        [RavenShardedAction("/databases/*/docs", "DELETE")]
        public async Task Delete()
        {
            var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
            
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var index = DatabaseContext.GetShardNumber(context, id);

                var cmd = new ShardedCommand(this, Headers.IfMatch);
                await DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(context, cmd, index);
            }

            NoContentStatus();
        }

        [RavenShardedAction("/databases/*/docs/class", "GET")]
        public async Task GenerateClassFromDocument()
        {
            var id = GetStringQueryString("id");
            var lang = (GetStringQueryString("lang", required: false) ?? "csharp")
                .Trim().ToLowerInvariant();

            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var index = DatabaseContext.GetShardNumber(context, id);

                var cmd = new ShardedCommand(this, Headers.None);
                await DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(context, cmd, index);
                var document = cmd.Result;
                if (document == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                switch (lang)
                {
                    case "csharp":
                        break;
                    default:
                        throw new NotImplementedException($"Document code generator isn't implemented for {lang}");
                }

                await using (var writer = new StreamWriter(ResponseBodyStream()))
                {
                    var codeGenerator = new JsonClassGenerator(lang);
                    var code = codeGenerator.Execute(document);
                    await writer.WriteAsync(code);
                }
            }
        }

        private async Task GetDocumentsAsync(TransactionOperationContext context, bool metadataOnly)
        {
            var token = ContinuationTokens.GetOrCreateContinuationToken(context);

            /*
            var databaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(context);
            if (GetStringFromHeaders("If-None-Match") == databaseChangeVector)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return;
            }
            HttpContext.Response.Headers["ETag"] = "\"" + databaseChangeVector + "\"";*/

            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Major, "Support returning Not Modified");

            var etag = GetLongQueryString("etag", false);
            if (etag != null)
                throw new NotSupportedException("Passing etag to a sharded database is not supported");

            ShardedCollectionHandler.ShardedStreamDocumentsOperation op;
            
            var isStartsWith = HttpContext.Request.Query.ContainsKey("startsWith");
            if (isStartsWith)
            {
                op = new ShardedCollectionHandler.ShardedStreamDocumentsOperation(
                    HttpContext,
                    HttpContext.Request.Query["startsWith"],
                    HttpContext.Request.Query["matches"],
                    HttpContext.Request.Query["exclude"],
                    HttpContext.Request.Query["startAfter"], 
                    token);
            }
            else // recent docs
            {
                op = new ShardedCollectionHandler.ShardedStreamDocumentsOperation(HttpContext, token);
            }

            var sw = Stopwatch.StartNew();
            var results = await ShardExecutor.ExecuteParallelForAllAsync(op);
            using var streams = await results.InitializeAsync(DatabaseContext, HttpContext.RequestAborted);
            var documents = DatabaseContext.Streaming.GetDocumentsAsync(streams, token);

            long numberOfResults;
            long totalDocumentsSizeInBytes;

            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Results");

                (numberOfResults, totalDocumentsSizeInBytes) = await writer.WriteDocumentsAsync(context, documents, metadataOnly, HttpContext.RequestAborted);
                writer.WriteComma();
                writer.WriteContinuationToken(context, token);

                writer.WriteEndObject();
            }

            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "Add paging performance hint");
            //AddPagingPerformanceHint(PagingOperationType.Documents, isStartsWith ? nameof(DocumentsStorage.GetDocumentsStartingWith) : nameof(GetDocumentsAsync), HttpContext.Request.QueryString.Value, numberOfResults, pageSize, sw.ElapsedMilliseconds, totalDocumentsSizeInBytes);
        }

        private async Task GetDocumentsByIdAsync(StringValues ids, StringValues includePaths, string etag, bool metadataOnly, TransactionOperationContext context)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "make sure we maintain the order of returned results");
            var idsByShard = ShardLocator.GetDocumentIdsByShards(context, DatabaseContext, ids);
            var op = new FetchDocumentsFromShardsOperation(context, this, idsByShard, etag, includePaths, metadataOnly);
            var shardedReadResult = await DatabaseContext.ShardExecutor.ExecuteParallelForShardsAsync(idsByShard.Keys.ToArray(), op);

            // here we know that all of them are good
            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
            if (shardedReadResult.StatusCode == (int)HttpStatusCode.NotModified)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return;
            }

            HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + shardedReadResult.CombinedEtag + "\"";
            var result = shardedReadResult.Result;

            await HandleMissingIncludes(context, metadataOnly, shardedReadResult.Result);

            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream(), ServerStore.ServerShutdown))
            {
                writer.WriteStartObject();

                await writer.WriteArrayAsync(nameof(GetDocumentsResult.Results), result.Results);

                writer.WriteComma();

                await WriteIncludesAsync(result.Includes, writer);

                writer.WriteEndObject();
                await writer.OuterFlushAsync();
                //TODO - sharding: Add performance hints
            }
        }

        private async Task HandleMissingIncludes(TransactionOperationContext context, bool metadataOnly, GetShardedDocumentsResult result)
        {
            if (result.MissingIncludes.Count > 0)
            {
                var ids = result.MissingIncludes;
                var idsByShard = ShardLocator.GetDocumentIdsByShards(context, DatabaseContext, ids);
                var op = new FetchDocumentsFromShardsOperation(context, this, idsByShard, etag: null, includePaths: null, metadataOnly: metadataOnly);
                var missingResult = await DatabaseContext.ShardExecutor.ExecuteParallelForShardsAsync(idsByShard.Keys.ToArray(), op);
                foreach (var missing in missingResult.Result.Results)
                {
                    if (missing == null)
                        continue;

                    var id = missing.GetMetadata().GetId();
                    result.Includes.Add(id, missing);
                }
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
    }
}
