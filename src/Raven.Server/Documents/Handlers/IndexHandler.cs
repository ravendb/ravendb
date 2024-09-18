using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Jint.Native;
using Jint.Native.Object;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents.Handlers.Processors.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Patch;
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
    public sealed class IndexHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/indexes/replace", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Replace()
        {
            using (var processor = new IndexHandlerProcessorForReplace(this))
                await processor.ExecuteAsync();
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

            using (var token = CreateHttpRequestBoundOperationToken())
                await Database.RachisLogIndexNotifications.WaitForIndexNotification(result.Index, token.Token);

            NoContentStatus();
        }

        [RavenAction("/databases/*/indexes/source", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Source()
        {
            using (var processor = new IndexHandlerProcessorForSource(this))
                await processor.ExecuteAsync();
        }

        public sealed class IndexHistoryResult
        {
            public string Index { get; set; }
            public IndexHistoryEntry[] History { get; set; }
        }

        [RavenAction("/databases/*/indexes/history", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetIndexHistory()
        {
            using (var processor = new IndexHandlerProcessorForGetIndexHistory<DocumentsOperationContext>(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/indexes/has-changed", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task HasChanged()
        {
            using (var processor = new IndexHandlerProcessorForHasChanged(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/indexes/debug", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Debug()
        {
            using (var processor = new IndexHandlerProcessorForDebug(this))
                await processor.ExecuteAsync();
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
            using (var processor = new IndexHandlerProcessorForTerms(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/indexes/total-time", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task TotalTime()
        {
            using (var processor = new IndexHandlerProcessorForTotalTime(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/indexes/performance", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task Performance()
        {
            using (var processor = new IndexHandlerProcessorForPerformance(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/indexes/performance/live", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, SkipUsagesCount = true)]
        public async Task PerformanceLive()
        {
            using (var processor = new IndexHandlerProcessorForPerformanceLive(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/indexes/suggest-index-merge", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task SuggestIndexMerge()
        {
            using (var processor = new IndexHandlerProcessorForSuggestIndexMerge(this))
                await processor.ExecuteAsync();
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
                        var docs = Database.DocumentsStorage.GetDocuments(context, listOfIds, 0, long.MaxValue);
                        foreach (var doc in docs)
                        {
                            if (doc.TryGetCollection(out string collectionStr))
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

        [RavenAction("/databases/*/indexes/debug/metadata", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task Metadata()
        {
            using (var context = QueryOperationContext.Allocate(Database, needsServerContext: true))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context.Documents, ServerStore, ResponseBodyStream()))
            {
                var indexMetadata = GetIndexesToReportOn()
                    .OrderBy(x => x.Name)
                    .Select(x =>
                    {
                        var timeFields = x._indexStorage.ReadIndexTimeFields();
                        var hasTimeFields = timeFields.Count > 0;

                        return new DynamicJsonValue
                        {
                            [nameof(x.Name)] = x.Name,
                            [nameof(x.Definition.Version)] = x.Definition.Version,
                            [nameof(x.Type)] = x.Type,
                            [nameof(x.Definition.State)] = x.Definition.State,
                            [nameof(x.Definition.LockMode)] = x.Definition.LockMode,
                            [nameof(x.SourceType)] = x.SourceType,
                            [nameof(x.Definition.Priority)] = x.Definition.Priority,
                            [nameof(x.SearchEngineType)] = x.SearchEngineType,
                            [nameof(x.Definition.HasDynamicFields)] = x.Definition.HasDynamicFields,
                            [nameof(x.Definition.HasCompareExchange)] = x.Definition.HasCompareExchange,
                            ["HasTimeFields"] = hasTimeFields,
                            ["TimeFields"] = timeFields
                        };
                    }).ToArray();
                
                writer.WriteStartObject();

                writer.WriteArray(context.Documents, "Results", indexMetadata, 
                    (w, c, metadata) => c.Write(w, metadata));

                writer.WriteEndObject();
            }
        }

        [RavenAction("/databases/*/indexes/auto/convert", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task ConvertAutoIndex()
        {
            using (var processor = new IndexHandlerProcessorForConvertAutoIndex<DatabaseRequestHandler, DocumentsOperationContext>(this))
                await processor.ExecuteAsync();
        }

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
