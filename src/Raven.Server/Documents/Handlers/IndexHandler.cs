using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Jint.Native;
using Jint.Native.Object;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents.Handlers.Admin.Processors.Indexes;
using Raven.Server.Documents.Handlers.Processors.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Debugging;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Index = Raven.Server.Documents.Indexes.Index;

namespace Raven.Server.Documents.Handlers
{
    public class IndexHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/indexes/replace", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public Task Replace()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            var replacementName = Constants.Documents.Indexing.SideBySideIndexNamePrefix + name;

            var oldIndex = Database.IndexStore.GetIndex(name);
            var newIndex = Database.IndexStore.GetIndex(replacementName);

            if (oldIndex == null && newIndex == null)
                throw new IndexDoesNotExistException($"Could not find '{name}' and '{replacementName}' indexes.");

            if (newIndex == null)
                throw new IndexDoesNotExistException($"Could not find side-by-side index for '{name}'.");

            using (var token = CreateOperationToken(TimeSpan.FromMinutes(15)))
            {
                Database.IndexStore.ReplaceIndexes(name, newIndex.Name, token.Token);
            }

            NoContentStatus();

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/indexes/finish-rolling", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task FinishRolling()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var node = GetStringQueryString("node", required: false);

            var index = Database.IndexStore.GetIndex(name);

            if (index == null)
                throw new IndexDoesNotExistException($"Could not find '{name}' index.");

            if (index.IsRolling == false)
                throw new InvalidOperationException($"'{name}' isn't a rolling index");

            var command = node == null ?
                new PutRollingIndexCommand(Database.Name, index.NormalizedName, Database.Time.GetUtcNow(), RaftIdGenerator.NewId()) :
                new PutRollingIndexCommand(Database.Name, index.NormalizedName, node, Database.Time.GetUtcNow(), RaftIdGenerator.NewId());

            var result = await ServerStore.SendToLeaderAsync(command);

            await Database.RachisLogIndexNotifications.WaitForIndexNotification(result.Index, HttpContext.RequestAborted);

            NoContentStatus();
        }

        [RavenAction("/databases/*/indexes/source", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Source()
        {
            using (var processor = new IndexHandlerProcessorForSource(this))
                await processor.ExecuteAsync();
        }

        public class IndexHistoryResult
        {
            public string Index { get; set; }
            public IndexHistoryEntry[] History { get; set; }
        }

        [RavenAction("/databases/*/indexes/history", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetIndexHistory()
        {
            using (var processor = new IndexHandlerProcessorForGetIndexHistory<DocumentsOperationContext>(this, ContextPool, Database.Name))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/indexes/has-changed", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task HasChanged()
        {
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var json = await context.ReadForMemoryAsync(RequestBodyStream(), "index/definition"))
            {
                var indexDefinition = JsonDeserializationServer.IndexDefinition(json);

                if (indexDefinition?.Name == null || indexDefinition.Maps.Count == 0)
                    throw new BadRequestException("Index definition must contain name and at least one map.");

                var changed = Database.IndexStore.HasChanged(indexDefinition);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Changed");
                    writer.WriteBool(changed);
                    writer.WriteEndObject();
                }
            }
        }

        [RavenAction("/databases/*/indexes/debug", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Debug()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            var index = Database.IndexStore.GetIndex(name);
            if (index == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }

            var operation = GetStringQueryString("op");

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                if (string.Equals(operation, "map-reduce-tree", StringComparison.OrdinalIgnoreCase))
                {
                    if (index.Type.IsMapReduce() == false)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;

                        context.Write(writer, new DynamicJsonValue
                        {
                            ["Error"] = $"{index.Name} is not map-reduce index"
                        });

                        return;
                    }

                    var docIds = GetStringValuesQueryString("docId", required: false);

                    using (index.GetReduceTree(docIds.ToArray(), out IEnumerable<ReduceTree> trees))
                    {
                        writer.WriteReduceTrees(trees);
                    }

                    return;
                }

                if (string.Equals(operation, "source-doc-ids", StringComparison.OrdinalIgnoreCase))
                {
                    using (index.GetIdentifiersOfMappedDocuments(GetStringQueryString("startsWith", required: false), GetStart(), GetPageSize(), out IEnumerable<string> ids))
                    {
                        writer.WriteArrayOfResultsAndCount(ids);
                    }

                    return;
                }

                if (string.Equals(operation, "entries-fields", StringComparison.OrdinalIgnoreCase))
                {
                    var fields = index.GetEntriesFields();

                    writer.WriteStartObject();

                    writer.WriteArray(nameof(fields.Static), fields.Static);
                    writer.WriteComma();

                    writer.WriteArray(nameof(fields.Dynamic), fields.Dynamic);

                    writer.WriteEndObject();

                    return;
                }

                throw new NotSupportedException($"{operation} is not supported");
            }
        }

        [RavenAction("/databases/*/indexes", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task GetAll()
        {
            var namesOnly = GetBoolValueQueryString("namesOnly", required: false) ?? false;

            if (namesOnly)
            {
                using (var processor = new IndexHandlerProcessorForGetAllNames(this))
                    await processor.ExecuteAsync();

                return;
            }

            using (var processor = new IndexHandlerProcessorForGetAll(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/indexes/stats", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task Stats()
        {
            using (var processor = new IndexHandlerProcessorForGetDatabaseIndexStatistics(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/indexes/staleness", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Stale()
        {
            using (var processor = new IndexHandlerProcessorForStale(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/indexes/progress", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Progress()
        {
            using (var processor = new IndexHandlerProcessorForProgress(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/indexes", "RESET", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task Reset()
        {
            using (var processor = new IndexHandlerProcessorForReset(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/index/open-faulty-index", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task OpenFaultyIndex()
        {
            using (var processor = new IndexHandlerProcessorForOpenFaultyIndex(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/indexes", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Delete()
        {
            using (var processor = new IndexHandlerProcessorForDelete(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/indexes/c-sharp-index-definition", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GenerateCSharpIndexDefinition()
        {
            using (var processor = new IndexProcessorForGenerateCSharpIndexDefinition(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/indexes/status", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Status()
        {
            using (var processor = new IndexHandlerProcessorForGetIndexesStatus(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/indexes/set-lock", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task SetLockMode()
        {
            using (var processor = new IndexHandlerProcessorForSetLockMode(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/indexes/set-priority", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task SetPriority()
        {
            using (var processor = new IndexHandlerProcessorForSetPriority(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/indexes/errors", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task ClearErrors()
        {
            using (var processor = new IndexHandlerProcessorForClearErrors(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/indexes/errors", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task GetErrors()
        {
            using (var processor = new IndexHandlerProcessorForGetErrors(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/indexes/terms", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task Terms()
        {
            using (var processor = new IndexHandlerProcessorForTerms(this, token: CreateTimeLimitedOperationToken(), existingResultEtag: GetLongFromHeaders("If-None-Match")))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/indexes/total-time", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task TotalTime()
        {
            var indexes = GetIndexesToReportOn();
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var dja = new DynamicJsonArray();

                foreach (var index in indexes)
                {
                    DateTime baseLine = DateTime.MinValue;
                    using (context.OpenReadTransaction())
                    {
                        foreach (var collection in index.Collections)
                        {
                            switch (index.SourceType)
                            {
                                case IndexSourceType.Documents:
                                    var etag = Database.DocumentsStorage.GetLastDocumentEtag(context.Transaction.InnerTransaction, collection);
                                    var document = Database.DocumentsStorage.GetDocumentsFrom(context, collection, etag, 0, 1, DocumentFields.Default).FirstOrDefault();
                                    if (document != null && document.LastModified > baseLine)
                                        baseLine = document.LastModified;
                                    break;

                                case IndexSourceType.Counters:
                                case IndexSourceType.TimeSeries:
                                    break;

                                default:
                                    throw new NotSupportedException($"Index with source type '{index.SourceType}' is not supported.");
                            }
                        }
                    }
                    var createdTimestamp = index.GetStats().CreatedTimestamp;
                    if (createdTimestamp > baseLine)
                        baseLine = createdTimestamp;

                    var lastBatch = index.GetIndexingPerformance()
                                    .LastOrDefault(x => x.Completed != null)
                                    ?.Completed ?? DateTime.UtcNow;

                    dja.Add(new DynamicJsonValue
                    {
                        ["Name"] = index.Name,
                        ["TotalIndexingTime"] = index.TimeSpentIndexing.Elapsed.ToString("c"),
                        ["LagTime"] = (lastBatch - baseLine).ToString("c")
                    });
                }

                context.Write(writer, dja);
            }
        }

        [RavenAction("/databases/*/indexes/performance", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Performance()
        {
            using (var processor = new IndexHandlerProcessorForPerformance(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/indexes/performance/live", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, SkipUsagesCount = true)]
        public async Task PerformanceLive()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                var indexNames = GetIndexesToReportOn().Select(x => x.Name).ToList();
                if (GetBoolValueQueryString("includeSideBySide", false) ?? false)
                {
                    // user requested to track side by side indexes as well
                    // add extra names to indexNames list
                    var complementaryIndexes = new HashSet<string>();
                    foreach (var indexName in indexNames)
                    {
                        if (indexName.StartsWith(Constants.Documents.Indexing.SideBySideIndexNamePrefix, StringComparison.OrdinalIgnoreCase))
                            complementaryIndexes.Add(indexName.Substring(Constants.Documents.Indexing.SideBySideIndexNamePrefix.Length));
                        else
                            complementaryIndexes.Add(Constants.Documents.Indexing.SideBySideIndexNamePrefix + indexName);
                    }

                    indexNames.AddRange(complementaryIndexes);
                }

                var receiveBuffer = new ArraySegment<byte>(new byte[1024]);
                var receive = webSocket.ReceiveAsync(receiveBuffer, Database.DatabaseShutdown);

                await using (var ms = new MemoryStream())
                using (var collector = new LiveIndexingPerformanceCollector(Database, indexNames))
                {
                    // 1. Send data to webSocket without making UI wait upon opening webSocket
                    await collector.SendStatsOrHeartbeatToWebSocket(receive, webSocket, ContextPool, ms, 100);

                    // 2. Send data to webSocket when available
                    while (Database.DatabaseShutdown.IsCancellationRequested == false)
                    {
                        if (await collector.SendStatsOrHeartbeatToWebSocket(receive, webSocket, ContextPool, ms, 4000) == false)
                        {
                            break;
                        }
                    }
                }
            }
        }

        [RavenAction("/databases/*/indexes/suggest-index-merge", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task SuggestIndexMerge()
        {
            var mergeIndexSuggestions = Database.IndexStore.ProposeIndexMergeSuggestions();

            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, mergeIndexSuggestions.ToJson());
            }
        }

        [RavenAction("/databases/*/indexes/try", "POST", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task TestJavaScriptIndex()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var input = await context.ReadForMemoryAsync(RequestBodyStream(), "TestJavaScriptIndex");
                if (input.TryGet("Definition", out BlittableJsonReaderObject index) == false)
                    ThrowRequiredPropertyNameInRequest("Definition");

                input.TryGet("Ids", out BlittableJsonReaderArray ids);

                var indexDefinition = JsonDeserializationServer.IndexDefinition(index);

                if (indexDefinition.Maps == null || indexDefinition.Maps.Count == 0)
                    throw new ArgumentException("Index must have a 'Maps' fields");

                indexDefinition.Type = indexDefinition.DetectStaticIndexType();

                if (indexDefinition.Type.IsJavaScript() == false)
                    throw new UnauthorizedAccessException("Testing indexes is only allowed for JavaScript indexes.");

                var compiledIndex = new JavaScriptIndex(indexDefinition, Database.Configuration, IndexDefinitionBaseServerSide.IndexVersion.CurrentVersion);

                var inputSize = GetIntValueQueryString("inputSize", false) ?? DefaultInputSizeForTestingJavaScriptIndex;
                var collections = new HashSet<string>(compiledIndex.Maps.Keys);
                var docsPerCollection = new Dictionary<string, List<DynamicBlittableJson>>();
                using (context.OpenReadTransaction())
                {
                    if (ids == null)
                    {
                        foreach (var collection in collections)
                        {
                            docsPerCollection.Add(collection,
                                Database.DocumentsStorage.GetDocumentsFrom(context, collection, 0, 0, inputSize).Select(d => new DynamicBlittableJson(d)).ToList());
                        }
                    }
                    else
                    {
                        var listOfIds = ids.Select(x => x.ToString());
                        var _ = new Reference<int>
                        {
                            Value = 0
                        };
                        var docs = Database.DocumentsStorage.GetDocuments(context, listOfIds, 0, long.MaxValue, _);
                        foreach (var doc in docs)
                        {
                            if (doc.TryGetMetadata(out var metadata) && metadata.TryGet(Constants.Documents.Metadata.Collection, out string collectionStr))
                            {
                                if (docsPerCollection.TryGetValue(collectionStr, out var listOfDocs) == false)
                                {
                                    listOfDocs = docsPerCollection[collectionStr] = new List<DynamicBlittableJson>();
                                }
                                listOfDocs.Add(new DynamicBlittableJson(doc));
                            }
                        }
                    }

                    var mapRes = new List<ObjectInstance>();
                    //all maps
                    foreach (var listOfFunctions in compiledIndex.Maps)
                    {
                        //multi maps per collection
                        foreach (var kvp in listOfFunctions.Value)
                        {
                            // TODO [ppekrol] check if this is correct
                            foreach (var mapFunc in kvp.Value)
                            {
                                if (docsPerCollection.TryGetValue(listOfFunctions.Key, out var docs))
                                {
                                    foreach (var res in mapFunc(docs))
                                    {
                                        mapRes.Add((ObjectInstance)res);
                                    }
                                }
                            }
                        }
                    }
                    var first = true;
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteStartObject();
                        writer.WritePropertyName("MapResults");
                        writer.WriteStartArray();
                        foreach (var mapResult in mapRes)
                        {
                            if (JavaScriptIndexUtils.StringifyObject(mapResult) is JsString jsStr)
                            {
                                if (first == false)
                                {
                                    writer.WriteComma();
                                }
                                writer.WriteString(jsStr.ToString());
                                first = false;
                            }
                        }
                        writer.WriteEndArray();
                        if (indexDefinition.Reduce != null)
                        {
                            using (var bufferPool = new UnmanagedBuffersPoolWithLowMemoryHandling("JavaScriptIndexTest", Database.Name))
                            {
                                compiledIndex.SetBufferPoolForTestingPurposes(bufferPool);
                                compiledIndex.SetAllocatorForTestingPurposes(context.Allocator);
                                first = true;
                                writer.WritePropertyName("ReduceResults");
                                writer.WriteStartArray();

                                var reduceResults = compiledIndex.Reduce(mapRes.Select(mr => new DynamicBlittableJson(JsBlittableBridge.Translate(context, mr.Engine, mr))));

                                foreach (JsValue reduceResult in reduceResults)
                                {
                                    if (JavaScriptIndexUtils.StringifyObject(reduceResult) is JsString jsStr)
                                    {
                                        if (first == false)
                                        {
                                            writer.WriteComma();
                                        }

                                        writer.WriteString(jsStr.ToString());
                                        first = false;
                                    }
                                }
                            }

                            writer.WriteEndArray();
                        }
                        writer.WriteEndObject();
                    }
                }
            }
        }

        private static readonly int DefaultInputSizeForTestingJavaScriptIndex = 10;

        private IEnumerable<Index> GetIndexesToReportOn()
        {
            IEnumerable<Index> indexes;
            var names = HttpContext.Request.Query["name"];

            if (names.Count == 0)
                indexes = Database.IndexStore
                    .GetIndexes();
            else
            {
                indexes = Database.IndexStore
                    .GetIndexes()
                    .Where(x => names.Contains(x.Name, StringComparer.OrdinalIgnoreCase));
            }
            return indexes;
        }
    }
}
