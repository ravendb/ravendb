using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Debugging;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Queries;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class IndexHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/indexes", "PUT")]
        public async Task Put()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var json = await context.ReadForDiskAsync(RequestBodyStream(), name);
                var indexDefinition = JsonDeserializationServer.IndexDefinition(json);
                if (indexDefinition.Maps == null || indexDefinition.Maps.Count == 0)
                    throw new ArgumentException("Index must have a 'Maps' fields");

                indexDefinition.Name = name;

                var indexId = Database.IndexStore.CreateIndex(indexDefinition);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName(("Index"));
                    writer.WriteString((name));
                    writer.WriteComma();

                    writer.WritePropertyName(("IndexId"));
                    writer.WriteInteger(indexId);

                    writer.WriteEndObject();
                }
            }
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
                    var staticMapIndex = (StaticMapIndex)index;
                    source = staticMapIndex._compiled.Source;
                    break;
                case IndexType.MapReduce:
                    var staticMapReduceIndex = (MapReduceIndex)index;
                    source = staticMapReduceIndex._compiled.Source;
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
                    ["Index"] = name,
                    ["Source"] = source
                });
            }

            return Task.CompletedTask;
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

                writer.WriteStartArray();

                var isFirst = true;
                foreach (var indexDefinition in indexDefinitions)
                {
                    if (isFirst == false)
                        writer.WriteComma();

                    isFirst = false;

                    if (namesOnly)
                    {
                        writer.WriteString((indexDefinition.Name));
                        continue;
                    }

                    writer.WriteIndexDefinition(context, indexDefinition);
                }

                writer.WriteEndArray();
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
                            throw new InvalidOperationException("There is not index with name: " + name);

                        indexStats = new[] { index.GetStats(calculateLag: true, calculateStaleness: true, documentsContext: context) };
                    }
                }

                writer.WriteStartArray();
                var first = true;
                foreach (var stats in indexStats)
                {
                    if (first == false)
                        writer.WriteComma();

                    first = false;
                    writer.WriteIndexStats(context, stats);
                }

                writer.WriteEndArray();

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
                    throw new InvalidOperationException("There is not index with name: " + name);

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
                writer.WritePropertyName(("IndexId"));
                writer.WriteInteger(newIndexId);
                writer.WriteEndObject();
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/indexes", "DELETE")]
        public Task Delete()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            HttpContext.Response.StatusCode = Database.IndexStore.TryDeleteIndexIfExists(name)
                ? (int)HttpStatusCode.NoContent
                : (int)HttpStatusCode.NotFound;

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/indexes/stop", "POST")]
        public Task Stop()
        {
            var types = HttpContext.Request.Query["type"];
            var names = HttpContext.Request.Query["name"];
            if (types.Count == 0 && names.Count == 0)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;
                Database.IndexStore.StopIndexing();
                return Task.CompletedTask;
            }

            if (types.Count != 0 && names.Count != 0)
                throw new ArgumentException("Query string value 'type' and 'names' are mutually exclusive.");

            if (types.Count != 0)
            {
                if (types.Count != 1)
                    throw new ArgumentException("Query string value 'type' must appear exactly once");
                if (string.IsNullOrWhiteSpace(types[0]))
                    throw new ArgumentException("Query string value 'type' must have a non empty value");

                if (string.Equals(types[0], "map", StringComparison.OrdinalIgnoreCase))
                {
                    Database.IndexStore.StopMapIndexes();
                }
                else if (string.Equals(types[0], "map-reduce", StringComparison.OrdinalIgnoreCase))
                {
                    Database.IndexStore.StopMapReduceIndexes();
                }
                else
                {
                    throw new ArgumentException("Query string value 'type' can only be 'map' or 'map-reduce' but was " + types[0]);
                }
            }
            else if (names.Count != 0)
            {
                if (names.Count != 1)
                    throw new ArgumentException("Query string value 'name' must appear exactly once");
                if (string.IsNullOrWhiteSpace(names[0]))
                    throw new ArgumentException("Query string value 'name' must have a non empty value");

                Database.IndexStore.StopIndex(names[0]);
            }

            HttpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/indexes/start", "POST")]
        public Task Start()
        {
            var types = HttpContext.Request.Query["type"];
            var names = HttpContext.Request.Query["name"];
            if (types.Count == 0 && names.Count == 0)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;
                Database.IndexStore.StartIndexing();
                return Task.CompletedTask;
            }

            if (types.Count != 0 && names.Count != 0)
                throw new ArgumentException("Query string value 'type' and 'names' are mutually exclusive.");

            if (types.Count != 0)
            {
                if (types.Count != 1)
                    throw new ArgumentException("Query string value 'type' must appear exactly once");
                if (string.IsNullOrWhiteSpace(types[0]))
                    throw new ArgumentException("Query string value 'type' must have a non empty value");

                if (string.Equals(types[0], "map", StringComparison.OrdinalIgnoreCase))
                {
                    Database.IndexStore.StartMapIndexes();
                }
                else if (string.Equals(types[0], "map-reduce", StringComparison.OrdinalIgnoreCase))
                {
                    Database.IndexStore.StartMapReduceIndexes();
                }
            }
            else if (names.Count != 0)
            {
                if (names.Count != 1)
                    throw new ArgumentException("Query string value 'name' must appear exactly once");
                if (string.IsNullOrWhiteSpace(names[0]))
                    throw new ArgumentException("Query string value 'name' must have a non empty value");

                Database.IndexStore.StartIndex(names[0]);
            }

            HttpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;
            return Task.CompletedTask;
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
        public Task SetLockMode()
        {
            var names = GetStringValuesQueryString("name");
            var modeStr = GetQueryStringValueAndAssertIfSingleAndNotEmpty("mode");

            IndexLockMode mode;
            if (Enum.TryParse(modeStr, out mode) == false)
                throw new InvalidOperationException("Query string value 'mode' is not a valid mode: " + modeStr);

            foreach (var name in names)
            {
                var index = Database.IndexStore.GetIndex(name);
                if (index == null)
                    throw new InvalidOperationException("There is not index with name: " + name);

                index.SetLock(mode);
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/indexes/set-priority", "POST")]
        public Task SetPriority()
        {
            var names = GetStringValuesQueryString("name");
            var priorityStr = GetQueryStringValueAndAssertIfSingleAndNotEmpty("priority");

            IndexingPriority priority;
            if (Enum.TryParse(priorityStr, out priority) == false)
                throw new InvalidOperationException("Query string value 'priority' is not a valid priority: " + priorityStr);

            foreach (var name in names)
            {
                var index = Database.IndexStore.GetIndex(name);
                if (index == null)
                    throw new InvalidOperationException("There is not index with name: " + name);

                index.SetPriority(priority);
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/indexes/errors", "GET")]
        public Task GetErrors()
        {
            var names = HttpContext.Request.Query["name"];

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
                        throw new InvalidOperationException("There is not index with name: " + name);

                    indexes.Add(index);
                }
            }

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartArray();

                var first = true;
                foreach (var index in indexes)
                {
                    if (first == false)
                    {
                        writer.WriteComma();
                    }

                    first = false;

                    writer.WriteStartObject();

                    writer.WritePropertyName(("Name"));
                    writer.WriteString((index.Name));
                    writer.WriteComma();

                    writer.WritePropertyName(("Errors"));
                    writer.WriteStartArray();
                    var firstError = true;
                    foreach (var error in index.GetErrors())
                    {
                        if (firstError == false)
                        {
                            writer.WriteComma();
                        }

                        firstError = false;

                        writer.WriteStartObject();

                        writer.WritePropertyName((nameof(error.Timestamp)));
                        writer.WriteString((error.Timestamp.GetDefaultRavenFormat()));
                        writer.WriteComma();

                        writer.WritePropertyName((nameof(error.Document)));
                        if (string.IsNullOrWhiteSpace(error.Document) == false)
                            writer.WriteString((error.Document));
                        else
                            writer.WriteNull();
                        writer.WriteComma();

                        writer.WritePropertyName((nameof(error.Action)));
                        if (string.IsNullOrWhiteSpace(error.Action) == false)
                            writer.WriteString((error.Action));
                        else
                            writer.WriteNull();
                        writer.WriteComma();

                        writer.WritePropertyName((nameof(error.Error)));
                        if (string.IsNullOrWhiteSpace(error.Error) == false)
                            writer.WriteString((error.Error));
                        else
                            writer.WriteNull();

                        writer.WriteEndObject();
                    }

                    writer.WriteEndArray();

                    writer.WriteEndObject();
                }

                writer.WriteEndArray();
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

                var result = runner.ExecuteGetTermsQuery(name, field, fromValue, existingResultEtag, GetPageSize(Database.Configuration.Core.MaxPageSize), context, token);

                if (result.NotModified)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return Task.CompletedTask;
                }

                HttpContext.Response.Headers[Constants.MetadataEtagField] = result.ResultEtag.ToInvariantString();

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartArray();
                    var isFirst = true;
                    foreach (var term in result.Terms)
                    {
                        if (isFirst == false)
                            writer.WriteComma();

                        isFirst = false;

                        writer.WriteString((term));
                    }

                    writer.WriteEndArray();
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

                    var lastBatch = index.GetIndexingPerformance(0)
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
            var from = 0;
            var froms = HttpContext.Request.Query["from"];
            if (froms.Count > 1)
                throw new ArgumentException($"Query string value 'from' must appear exactly once");
            if (froms.Count > 0 && int.TryParse(froms[0], out from) == false)
                throw new ArgumentException($"Query string value 'from' must be a number");

            var stats = GetIndexesToReportOn()
                .Select(x => new IndexPerformanceStats
                {
                    IndexName = x.Name,
                    IndexId = x.IndexId,
                    Performance = x.GetIndexingPerformance(from)
                })
                .ToArray();

            JsonOperationContext context;
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartArray();

                var isFirst = true;
                foreach (var stat in stats)
                {
                    if (isFirst == false)
                        writer.WriteComma();

                    isFirst = false;

                    writer.WriteStartObject();

                    writer.WritePropertyName(nameof(stat.IndexName));
                    writer.WriteString((stat.IndexName));
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(stat.IndexId));
                    writer.WriteInteger(stat.IndexId);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(stat.Performance));
                    writer.WriteStartArray();
                    var isFirstInternal = true;
                    foreach (var performance in stat.Performance)
                    {
                        if (isFirstInternal == false)
                            writer.WriteComma();

                        isFirstInternal = false;

                        writer.WriteIndexingPerformanceStats(context, performance);
                    }
                    writer.WriteEndArray();

                    writer.WriteEndObject();
                }

                writer.WriteEndArray();
            }

            return Task.CompletedTask;
        }

        private IEnumerable<Index> GetIndexesToReportOn()
        {
            IEnumerable<Index> indexes;
            var names = HttpContext.Request.Query["name"];

            if (names.Count == 0)
                indexes = Database.IndexStore
                    .GetIndexes()
                    .OrderBy(x => x.IndexId);
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