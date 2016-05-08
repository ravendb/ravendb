using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class IndexHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/indexes/source", "GET")]
        public Task Source()
        {
            var names = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            var index = Database.IndexStore.GetIndex(names[0]);
            if (index == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return Task.CompletedTask;
            }

            throw new NotImplementedException(); // TODO [ppekrol] need static indexes
        }

        [RavenAction("/databases/*/indexes/debug", "GET")]
        public Task Debug()
        {
            var names = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            var index = Database.IndexStore.GetIndex(names[0]);
            if (index == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return Task.CompletedTask;
            }

            throw new NotImplementedException(); // TODO [ppekrol] not sure yet what will be needed, let's wait for Studio
        }

        [RavenAction("/databases/*/indexes", "GET")]
        public Task GetAll()
        {
            var names = HttpContext.Request.Query["name"];
            if (names.Count > 1)
                throw new ArgumentException($"Query string value 'name' must appear exactly once");

            var start = GetStart();
            var pageSize = GetPageSize();
            var namesOnly = GetBoolValueQueryString("namesOnly", required: false) ?? false;

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                IndexDefinition[] indexDefinitions;
                if (names.Count == 0)
                    indexDefinitions = Database.IndexStore
                        .GetIndexes()
                        .OrderBy(x => x.IndexId)
                        .Skip(start)
                        .Take(pageSize)
                        .Select(x => x.GetIndexDefinition())
                        .ToArray();
                else
                {
                    var index = Database.IndexStore.GetIndex(names[0]);
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
                        writer.WriteString(context.GetLazyString(indexDefinition.Name));
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
            var names = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            var index = Database.IndexStore.GetIndex(names[0]);
            if (index == null)
                throw new InvalidOperationException("There is not index with name: " + names[0]);

            var stats = index.GetStats();

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteIndexStats(context, stats);
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/indexes", "RESET")]
        public Task Reset()
        {
            var names = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            var newIndexId = Database.IndexStore.ResetIndex(names[0]);

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(context.GetLazyString("IndexId"));
                writer.WriteInteger(newIndexId);
                writer.WriteEndObject();
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/indexes", "DELETE")]
        public Task Delete()
        {
            var names = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            Database.IndexStore.DeleteIndex(names[0]);

            HttpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;
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

        [RavenAction("/databases/*/indexes/status", "GET")]
        public Task Status()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartArray();
                var isFirst = true;
                foreach (var index in Database.IndexStore.GetIndexes())
                {
                    if (isFirst == false)
                        writer.WriteComma();

                    isFirst = false;

                    writer.WriteStartObject();

                    writer.WritePropertyName(context.GetLazyString("Name"));
                    writer.WriteString(context.GetLazyString(index.Name));

                    writer.WriteComma();

                    writer.WritePropertyName(context.GetLazyString("Status"));
                    string status;
                    if (Database.Configuration.Indexing.Disabled)
                        status = "Disabled";
                    else
                        status = index.IsRunning ? "Running" : "Paused";

                    writer.WriteString(context.GetLazyString(status));

                    writer.WriteEndObject();
                }

                writer.WriteEndArray();
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/indexes/set-lock", "POST")]
        public Task SetLockMode()
        {
            var names = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var modes = GetQueryStringValueAndAssertIfSingleAndNotEmpty("mode");

            IndexLockMode mode;
            if (Enum.TryParse(modes[0], out mode) == false)
                throw new InvalidOperationException("Query string value 'mode' is not a valid mode: " + modes[0]);

            var index = Database.IndexStore.GetIndex(names[0]);
            if (index == null)
                throw new InvalidOperationException("There is not index with name: " + names[0]);

            index.SetLock(mode);

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/indexes/set-priority", "POST")]
        public Task SetPriority()
        {
            var names = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var priorities = GetQueryStringValueAndAssertIfSingleAndNotEmpty("priority");

            IndexingPriority priority;
            if (Enum.TryParse(priorities[0], out priority) == false)
                throw new InvalidOperationException("Query string value 'priority' is not a valid priority: " + priorities[0]);

            var index = Database.IndexStore.GetIndex(names[0]);
            if (index == null)
                throw new InvalidOperationException("There is not index with name: " + names[0]);

            index.SetPriority(priority);

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

                    writer.WritePropertyName(context.GetLazyString("Name"));
                    writer.WriteString(context.GetLazyString(index.Name));
                    writer.WriteComma();

                    writer.WritePropertyName(context.GetLazyString("Errors"));
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

                        writer.WritePropertyName(context.GetLazyString(nameof(error.Timestamp)));
                        writer.WriteString(context.GetLazyString(error.Timestamp.GetDefaultRavenFormat()));
                        writer.WriteComma();

                        writer.WritePropertyName(context.GetLazyString(nameof(error.Document)));
                        if (string.IsNullOrWhiteSpace(error.Document) == false)
                            writer.WriteString(context.GetLazyString(error.Document));
                        else
                            writer.WriteNull();
                        writer.WriteComma();

                        writer.WritePropertyName(context.GetLazyString(nameof(error.Action)));
                        if (string.IsNullOrWhiteSpace(error.Action) == false)
                            writer.WriteString(context.GetLazyString(error.Action));
                        else
                            writer.WriteNull();
                        writer.WriteComma();

                        writer.WritePropertyName(context.GetLazyString(nameof(error.Error)));
                        if (string.IsNullOrWhiteSpace(error.Error) == false)
                            writer.WriteString(context.GetLazyString(error.Error));
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
            var names = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var fields = GetQueryStringValueAndAssertIfSingleAndNotEmpty("field");
            var fromValue = GetStringQueryString("fromValue", required: false);

            DocumentsOperationContext context;
            using (var token = CreateTimeLimitedOperationToken())
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var existingResultEtag = GetLongFromHeaders("If-None-Match");

                var runner = new QueryRunner(Database, context);

                var result = runner.ExecuteGetTermsQuery(names[0], fields[0], fromValue, existingResultEtag, GetPageSize(Database.Configuration.Core.MaxPageSize), context, token);

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

                        writer.WriteString(context.GetLazyString(term));
                    }

                    writer.WriteEndArray();
                }

                return Task.CompletedTask;
            }
        }

        [RavenAction("/databases/*/indexes/performance", "GET")]
        public Task Performance()
        {
            var names = HttpContext.Request.Query["name"];
            var froms = HttpContext.Request.Query["from"];
            var from = 0;
            if (froms.Count > 1)
                throw new ArgumentException($"Query string value 'from' must appear exactly once");
            if (froms.Count > 0 && int.TryParse(froms[0], out from) == false)
                throw new ArgumentException($"Query string value 'from' must be a number");

            IEnumerable<Index> indexes;

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

            var stats = indexes
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

                    writer.WritePropertyName(context.GetLazyString(nameof(stat.IndexName)));
                    writer.WriteString(context.GetLazyString(stat.IndexName));
                    writer.WriteComma();

                    writer.WritePropertyName(context.GetLazyString(nameof(stat.IndexId)));
                    writer.WriteInteger(stat.IndexId);
                    writer.WriteComma();

                    writer.WritePropertyName(context.GetLazyString(nameof(stat.Performance)));
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
    }
}