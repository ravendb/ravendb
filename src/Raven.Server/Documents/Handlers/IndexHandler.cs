using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.Debugging;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
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
                IndexDefinition indexDefinition;
                using (var json = await context.ReadForDiskAsync(RequestBodyStream(), name))
                {
                    indexDefinition = JsonDeserializationServer.IndexDefinition(json);
                }

                if (indexDefinition.Maps == null || indexDefinition.Maps.Count == 0)
                    throw new ArgumentException("Index must have a 'Maps' fields");

                indexDefinition.Name = name;

                var indexId = Database.IndexStore.CreateIndex(indexDefinition);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName("Index");
                    writer.WriteString(name);
                    writer.WriteComma();

                    writer.WritePropertyName("IndexId");
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
                    var staticMapIndex = (MapIndex)index;
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
                    ["Index"] = index.Name,
                    ["Source"] = source
                });
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/indexes/rename", "POST")]
        public Task Rename()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var newName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("newName");

            Thread.Sleep(2000);//TODO: implement me and remove this sleep!

            HttpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;
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
                    using (index.GetIdentifiersOfMappedDocuments(GetStringQueryString("startsWith", required: false), GetStart(), GetPageSize(Database.Configuration.Core.MaxPageSize), out ids))
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
            var pageSize = GetPageSize(Database.Configuration.Core.MaxPageSize);
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
                    IndexDoesNotExistsException.ThrowFor(name);

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

            HttpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;

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
        public Task Delete()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            HttpContext.Response.StatusCode = Database.IndexStore.TryDeleteIndexIfExists(name)
                ? (int)HttpStatusCode.NoContent
                : (int)HttpStatusCode.NotFound;

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
                    IndexDoesNotExistsException.ThrowFor(name);

                index.SetLock(mode);
            }

            HttpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/indexes/set-priority", "POST")]
        public Task SetPriority()
        {
            var names = GetStringValuesQueryString("name");
            var priorityStr = GetQueryStringValueAndAssertIfSingleAndNotEmpty("priority");

            IndexPriority priority;
            if (Enum.TryParse(priorityStr, out priority) == false)
                throw new InvalidOperationException("Query string value 'priority' is not a valid priority: " + priorityStr);

            foreach (var name in names)
            {
                var index = Database.IndexStore.GetIndex(name);
                if (index == null)
                    IndexDoesNotExistsException.ThrowFor(name);

                index.SetPriority(priority);
            }

            HttpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;
            return Task.CompletedTask;
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
                        IndexDoesNotExistsException.ThrowFor(name);

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
                        if (string.IsNullOrWhiteSpace(error.Document) == false)
                            ew.WriteString(error.Document);
                        else
                            ew.WriteNull();
                        ew.WriteComma();

                        ew.WritePropertyName(nameof(error.Action));
                        if (string.IsNullOrWhiteSpace(error.Action) == false)
                            ew.WriteString(error.Action);
                        else
                            ew.WriteNull();
                        ew.WriteComma();

                        ew.WritePropertyName(nameof(error.Error));
                        if (string.IsNullOrWhiteSpace(error.Error) == false)
                            ew.WriteString(error.Error);
                        else
                            ew.WriteNull();

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

                var result = runner.ExecuteGetTermsQuery(name, field, fromValue, existingResultEtag, GetPageSize(Database.Configuration.Core.MaxPageSize), context, token);

                if (result.NotModified)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return Task.CompletedTask;
                }

                HttpContext.Response.Headers[Constants.MetadataEtagField] = result.ResultEtag.ToInvariantString();

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
            var from = GetIntValueQueryString("from", required: false) ?? 0;

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
                writer.WriteArray(context, stats, (w, c, stat) =>
                {
                    w.WriteStartObject();

                    w.WritePropertyName(nameof(stat.IndexName));
                    w.WriteString(stat.IndexName);
                    w.WriteComma();

                    w.WritePropertyName(nameof(stat.IndexId));
                    w.WriteInteger(stat.IndexId);
                    w.WriteComma();

                    w.WritePropertyName(nameof(stat.Performance));
                    w.WriteArray(c, stat.Performance, (wp, cp, performance) =>
                    {
                        wp.WriteIndexingPerformanceStats(context, performance);
                    });

                    w.WriteEndObject();

                });
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