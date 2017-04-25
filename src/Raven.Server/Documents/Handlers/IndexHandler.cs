using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.WebSockets;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Exceptions.Indexes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Debugging;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Queries;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class IndexHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/indexes", "PUT")]
        public async Task Put()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var createdIndexes = new List<KeyValuePair<string, long>>();
                var tuple = await context.ParseArrayToMemoryAsync(RequestBodyStream(), "Indexes", BlittableJsonDocumentBuilder.UsageMode.None);
                using (tuple.Item2)
                {
                    foreach (var indexToAdd in tuple.Item1)
                    {
                        var indexDefinition = JsonDeserializationServer.IndexDefinition((BlittableJsonReaderObject)indexToAdd);

                        if (indexDefinition.Maps == null || indexDefinition.Maps.Count == 0)
                            throw new ArgumentException("Index must have a 'Maps' fields");
                        var etag = await Database.IndexStore.CreateIndex(indexDefinition);
                        createdIndexes.Add(new KeyValuePair<string, long>(indexDefinition.Name, etag));
                    }
                }

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WriteResults(context, createdIndexes, (w, c, index) =>
                    {
                        w.WriteStartObject();
                        w.WritePropertyName(nameof(PutIndexResult.IndexId));
                        w.WriteInteger(index.Value);

                        w.WriteComma();

                        w.WritePropertyName(nameof(PutIndexResult.Index));
                        w.WriteString(index.Key);
                        w.WriteEndObject();
                    });

                    writer.WriteEndObject();
                }
            }
        }

        [RavenAction("/databases/*/indexes/replace", "POST")]
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

            while (Database.DatabaseShutdown.IsCancellationRequested == false)
            {
                if (Database.IndexStore.TryReplaceIndexes(name, newIndex.Name))
                    break;
            }

            return NoContent();
        }

        [RavenAction("/databases/*/indexes/source", "GET")]
        public Task Source()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            var index = Database.IndexStore.GetIndex(name);
            if (index == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return Task.CompletedTask;
            }

            if (index.Type.IsStatic() == false)
                throw new InvalidOperationException("Source can be only retrieved for static indexes.");

            string source = null;
            switch (index.Type)
            {
                case IndexType.Map:
                    var staticMapIndex = (MapIndex)index;
                    source = staticMapIndex._compiled.Source;
                    break;
                case IndexType.MapReduce:
                    var staticMapReduceIndex = (MapReduceIndex)index;
                    source = staticMapReduceIndex.Compiled.Source;
                    break;
            }

            if (string.IsNullOrWhiteSpace(source))
                throw new InvalidOperationException("Could not retrieve source for given index.");

            JsonOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    ["Index"] = index.Name,
                    ["Source"] = source
                });
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/indexes/has-changed", "POST")]
        public Task HasChanged()
        {
            JsonOperationContext context;
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            using (var json = context.ReadForMemory(RequestBodyStream(), "index/definition"))
            {
                var indexDefinition = JsonDeserializationServer.IndexDefinition(json);

                if (indexDefinition?.Name == null || indexDefinition.Maps.Count == 0)
                    throw new BadRequestException("Index definition must contain name and at least one map.");

                var changed = Database.IndexStore.HasChanged(indexDefinition);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Changed");
                    writer.WriteBool(changed);
                    writer.WriteEndObject();
                }
            }

            return NoContent();
        }

        [RavenAction("/databases/*/indexes/rename", "POST")]
        public Task Rename()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var newName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("newName");

            Database.IndexStore.RenameIndex(name, newName);

            return NoContent();
        }

        [RavenAction("/databases/*/indexes/debug", "GET")]
        public Task Debug()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            var index = Database.IndexStore.GetIndex(name);
            if (index == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return Task.CompletedTask;
            }

            var operation = GetStringQueryString("op");

            JsonOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                if (string.Equals(operation, "map-reduce-tree", StringComparison.OrdinalIgnoreCase))
                {
                    if (index.Type.IsMapReduce() == false)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;

                        context.Write(writer, new DynamicJsonValue
                        {
                            ["Error"] = $"{index.Name} is not map-reduce index",
                        });

                        return Task.CompletedTask;
                    }

                    var docId = GetStringQueryString("docId", required: false);

                    IEnumerable<ReduceTree> trees;
                    using (index.GetReduceTree(docId, out trees))
                    {
                        writer.WriteReduceTrees(trees);
                    }

                    return Task.CompletedTask;
                }

                if (string.Equals(operation, "source-doc-ids", StringComparison.OrdinalIgnoreCase))
                {
                    IEnumerable<string> ids;
                    using (index.GetIdentifiersOfMappedDocuments(GetStringQueryString("startsWith", required: false), GetStart(), GetPageSize(), out ids))
                    {
                        writer.WriteArrayOfResultsAndCount(ids);
                    }

                    return Task.CompletedTask;
                }

                if (string.Equals(operation, "entries-fields", StringComparison.OrdinalIgnoreCase))
                {
                    var fields = index.GetEntriesFields();

                    var first = true;
                    writer.WriteStartArray();

                    foreach (var field in fields)
                    {
                        if (first == false)
                            writer.WriteComma();

                        first = false;

                        writer.WriteString(field);
                    }

                    writer.WriteEndArray();

                    return Task.CompletedTask;
                }

                throw new NotSupportedException($"{operation} is not supported");
            }
        }

        [RavenAction("/databases/*/indexes", "GET")]
        public Task GetAll()
        {
            var name = GetStringQueryString("name", required: false);

            var start = GetStart();
            var pageSize = GetPageSize();
            var namesOnly = GetBoolValueQueryString("namesOnly", required: false) ?? false;

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                IndexDefinition[] indexDefinitions;
                if (string.IsNullOrEmpty(name))
                    indexDefinitions = Database.IndexStore
                        .GetIndexes()
                        .OrderBy(x => x.Name)
                        .Skip(start)
                        .Take(pageSize)
                        .Select(x => x.GetIndexDefinition())
                        .ToArray();
                else
                {
                    var index = Database.IndexStore.GetIndex(name);
                    if (index == null)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return Task.CompletedTask;
                    }

                    indexDefinitions = new[] { index.GetIndexDefinition() };
                }

                writer.WriteStartObject();

                writer.WriteResults(context, indexDefinitions, (w, c, indexDefinition) =>
                {
                    if (namesOnly)
                    {
                        w.WriteString(indexDefinition.Name);
                        return;
                    }

                    w.WriteIndexDefinition(c, indexDefinition);
                });

                writer.WriteEndObject();
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/indexes/stats", "GET")]
        public Task Stats()
        {
            var name = GetStringQueryString("name", required: false);

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                IndexStats[] indexStats;
                using (context.OpenReadTransaction())
                {
                    if (string.IsNullOrEmpty(name))
                        indexStats = Database.IndexStore
                            .GetIndexes()
                            .OrderBy(x => x.Name)
                            .Select(x => x.GetStats(calculateLag: true, calculateStaleness: true, documentsContext: context))
                            .ToArray();
                    else
                    {
                        var index = Database.IndexStore.GetIndex(name);
                        if (index == null)
                        {
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            return Task.CompletedTask;
                        }

                        indexStats = new[] { index.GetStats(calculateLag: true, calculateStaleness: true, documentsContext: context) };
                    }
                }

                writer.WriteStartObject();

                writer.WriteResults(context, indexStats, (w, c, stats) =>
                {
                    w.WriteIndexStats(context, stats);
                });

                writer.WriteEndObject();
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/indexes/progress", "GET")]
        public Task Progress()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            using (context.OpenReadTransaction())
            {
                var index = Database.IndexStore.GetIndex(name);
                if (index == null)
                    IndexDoesNotExistException.ThrowFor(name);

                var progress = index.GetProgress(context);
                writer.WriteIndexProgress(context, progress);
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/indexes", "RESET")]
        public Task Reset()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            var newIndexId = Database.IndexStore.ResetIndex(name);

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("IndexId");
                writer.WriteInteger(newIndexId);
                writer.WriteEndObject();
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/indexes", "DELETE")]
        public async Task Delete()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            HttpContext.Response.StatusCode = await Database.IndexStore.TryDeleteIndexIfExists(name)
                ? (int)HttpStatusCode.NoContent
                : (int)HttpStatusCode.NotFound;
        }

        [RavenAction("/databases/*/indexes/c-sharp-index-definition", "GET")]
        public Task GenerateCSharpIndexDefinition()
        {
            var indexName = HttpContext.Request.Query["name"];
            var index = Database.IndexStore.GetIndex(indexName);
            if (index == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return Task.CompletedTask;
            }

            if (index.Type.IsAuto())
                throw new InvalidOperationException("Can't create C# index definition from auto indexes");

            var indexDefinition = index.GetIndexDefinition();

            using (var writer = new StreamWriter(ResponseBodyStream()))
            {
                var text = new IndexDefinitionCodeGenerator(indexDefinition).Generate();
                writer.Write(text);
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/indexes/status", "GET")]
        public Task Status()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName(nameof(IndexingStatus.Status));
                writer.WriteString(Database.IndexStore.Status.ToString());
                writer.WriteComma();

                writer.WritePropertyName(nameof(IndexingStatus.Indexes));
                writer.WriteStartArray();
                var isFirst = true;
                foreach (var index in Database.IndexStore.GetIndexes())
                {
                    if (isFirst == false)
                        writer.WriteComma();

                    isFirst = false;

                    writer.WriteStartObject();

                    writer.WritePropertyName(nameof(IndexingStatus.IndexStatus.Name));
                    writer.WriteString(index.Name);

                    writer.WriteComma();

                    writer.WritePropertyName(nameof(IndexingStatus.IndexStatus.Status));
                    writer.WriteString(index.Status.ToString());

                    writer.WriteEndObject();
                }

                writer.WriteEndArray();

                writer.WriteEndObject();
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/indexes/set-lock", "POST")]
        public async Task SetLockMode()
        {
            var names = GetStringValuesQueryString("name");
            var modeStr = GetQueryStringValueAndAssertIfSingleAndNotEmpty("mode");

            IndexLockMode mode;
            if (Enum.TryParse(modeStr, out mode) == false)
                throw new InvalidOperationException("Query string value 'mode' is not a valid mode: " + modeStr);

            foreach (var name in names)
            {
                await Database.IndexStore.SetLock(name, mode);
            }

            NoContentStatus();
        }

        [RavenAction("/databases/*/indexes/set-priority", "POST")]
        public async Task SetPriority()
        {
            var names = GetStringValuesQueryString("name");
            var priorityStr = GetQueryStringValueAndAssertIfSingleAndNotEmpty("priority");

            IndexPriority priority;
            if (Enum.TryParse(priorityStr, out priority) == false)
                throw new InvalidOperationException("Query string value 'priority' is not a valid priority: " + priorityStr);

            foreach (var name in names)
            {
                await Database.IndexStore.SetPriority(name, priority);
            }

            NoContentStatus();
        }

        [RavenAction("/databases/*/indexes/errors", "GET")]
        public Task GetErrors()
        {
            var names = GetStringValuesQueryString("name", required: false);

            List<Index> indexes;
            if (names.Count == 0)
                indexes = Database.IndexStore.GetIndexes().ToList();
            else
            {
                indexes = new List<Index>();
                foreach (var name in names)
                {
                    var index = Database.IndexStore.GetIndex(name);
                    if (index == null)
                        IndexDoesNotExistException.ThrowFor(name);

                    indexes.Add(index);
                }
            }
            
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteArray(context, indexes, (w, c, index) =>
                {
                    w.WriteStartObject();
                    w.WritePropertyName("Name");
                    w.WriteString(index.Name);
                    w.WriteComma();
                    w.WritePropertyName("Errors");
                    w.WriteArray(c, index.GetErrors(), (ew, ec, error) =>
                    {
                        ew.WriteStartObject();
                        ew.WritePropertyName(nameof(error.Timestamp));
                        ew.WriteString(error.Timestamp.GetDefaultRavenFormat());
                        ew.WriteComma();

                        ew.WritePropertyName(nameof(error.Document));
                        ew.WriteString(error.Document);
                        ew.WriteComma();

                        ew.WritePropertyName(nameof(error.Action));
                        ew.WriteString(error.Action); 
                        ew.WriteComma();

                        ew.WritePropertyName(nameof(error.Error));
                        ew.WriteString(error.Error);
                        ew.WriteEndObject();
                    });
                    w.WriteEndObject();
                });
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/indexes/terms", "GET")]
        public Task Terms()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var field = GetQueryStringValueAndAssertIfSingleAndNotEmpty("field");
            var fromValue = GetStringQueryString("fromValue", required: false);

            DocumentsOperationContext context;
            using (var token = CreateTimeLimitedOperationToken())
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var existingResultEtag = GetLongFromHeaders("If-None-Match");

                var runner = new QueryRunner(Database, context);

                var result = runner.ExecuteGetTermsQuery(name, field, fromValue, existingResultEtag, GetPageSize(), context, token);

                if (result.NotModified)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return Task.CompletedTask;
                }

                HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteTermsQueryResult(context, result);
                }

                return Task.CompletedTask;
            }
        }

        [RavenAction("/databases/*/indexes/total-time", "GET")]
        public Task TotalTime()
        {
            var indexes = GetIndexesToReportOn();
            DocumentsOperationContext context;
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var dja = new DynamicJsonArray();

                foreach (var index in indexes)
                {
                    DateTime baseLine = DateTime.MinValue;
                    using (context.OpenReadTransaction())
                    {
                        foreach (var collection in index.Collections)
                        {
                            var etag = Database.DocumentsStorage.GetLastDocumentEtag(context, collection);
                            var document = Database.DocumentsStorage.GetDocumentsFrom(context, collection, etag, 0, 1).FirstOrDefault();
                            if (document != null)
                            {
                                if (document.LastModified > baseLine)
                                    baseLine = document.LastModified;
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
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/indexes/performance", "GET")]
        public Task Performance()
        {
            var stats = GetIndexesToReportOn()
                .Select(x => new IndexPerformanceStats
                {
                    Name = x.Name,
                    Etag = x.Etag,
                    Performance = x.GetIndexingPerformance()
                })
                .ToArray();

            JsonOperationContext context;
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WritePerformanceStats(context, stats);
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/indexes/performance/live", "GET", SkipUsagesCount = true)]
        public async Task PerformanceLive()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                var indexes = GetIndexesToReportOn().ToArray();

                var receiveBuffer = new ArraySegment<byte>(new byte[1024]);
                var receive = webSocket.ReceiveAsync(receiveBuffer, Database.DatabaseShutdown);

                using (var ms = new MemoryStream())
                using (var collector = new LiveIndexingPerformanceCollector(Database, Database.DatabaseShutdown, indexes))
                {
                    while (Database.DatabaseShutdown.IsCancellationRequested == false)
                    {
                        if (receive.IsCompleted || webSocket.State != WebSocketState.Open)
                            break;

                        var tuple = await collector.Stats.TryDequeueAsync(TimeSpan.FromSeconds(4));
                        if (tuple.Item1 == false)
                        {
                            await webSocket.SendAsync(WebSocketHelper.Heartbeat, WebSocketMessageType.Text, true, Database.DatabaseShutdown);
                            continue;
                        }

                        ms.SetLength(0);

                        JsonOperationContext context;
                        using (ContextPool.AllocateOperationContext(out context))
                        using (var writer = new BlittableJsonTextWriter(context, ms))
                        {
                            writer.WritePerformanceStats(context, tuple.Item2);
                        }

                        ArraySegment<byte> bytes;
                        ms.TryGetBuffer(out bytes);

                        await webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, Database.DatabaseShutdown);
                    }
                }
            }
        }

        private IEnumerable<Index> GetIndexesToReportOn()
        {
            IEnumerable<Index> indexes;
            var names = HttpContext.Request.Query["name"];

            if (names.Count == 0)
                indexes = Database.IndexStore
                    .GetIndexes()
                    .OrderBy(x => x.Etag);
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