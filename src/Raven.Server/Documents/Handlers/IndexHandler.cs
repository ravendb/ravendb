using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers
{
    public class IndexHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/indexes", "GET")]
        public Task GetAll()
        {
            var start = GetStart();
            var pageSize = GetPageSize();

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartArray();

                var isFirst = true;
                foreach (var index in Database.IndexStore.GetIndexes().OrderBy(x => x.IndexId).Skip(start).Take(pageSize))
                {
                    if (isFirst == false)
                        writer.WriteComma();

                    isFirst = false;
                    writer.WriteStartObject();

                    writer.WritePropertyName(context.GetLazyString(nameof(IndexDefinition.Name)));
                    writer.WriteString(context.GetLazyString(index.Name));
                    writer.WriteComma();

                    writer.WritePropertyName(context.GetLazyString(nameof(IndexDefinition.IndexId)));
                    writer.WriteInteger(index.IndexId);

                    // TODO [ppekrol] more index definition fields

                    writer.WriteEndObject();
                }

                writer.WriteEndArray();
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/indexes/stats", "GET")]
        public Task Stats()
        {
            var names = HttpContext.Request.Query["name"];
            if (names.Count != 1)
                throw new ArgumentException("Query string value 'name' must appear exactly once");
            if (string.IsNullOrWhiteSpace(names[0]))
                throw new ArgumentException("Query string value 'name' must have a non empty value");

            var index = Database.IndexStore.GetIndex(names[0]);
            if (index == null)
                throw new InvalidOperationException("There is not index with name: " + names[0]);

            var stats = index.GetStats();

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName(context.GetLazyString(nameof(stats.ForCollections)));
                writer.WriteStartArray();
                var isFirst = true;
                foreach (var collection in stats.ForCollections)
                {
                    if (isFirst == false)
                        writer.WriteComma();

                    isFirst = false;
                    writer.WriteString(context.GetLazyString(collection));
                }
                writer.WriteEndArray();
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(stats.IsInMemory)));
                writer.WriteBool(stats.IsInMemory);
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(stats.LastIndexedEtags)));
                writer.WriteStartObject();
                isFirst = true;
                foreach (var kvp in stats.LastIndexedEtags)
                {
                    if (isFirst == false)
                        writer.WriteComma();

                    isFirst = false;

                    writer.WritePropertyName(context.GetLazyString(kvp.Key));
                    writer.WriteInteger(kvp.Value);
                }
                writer.WriteEndObject();
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(stats.LastIndexingTime)));
                if (stats.LastIndexingTime.HasValue)
                    writer.WriteString(context.GetLazyString(stats.LastIndexingTime.Value.GetDefaultRavenFormat(isUtc: true)));
                else
                    writer.WriteNull();
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(stats.LastQueryingTime)));
                if (stats.LastQueryingTime.HasValue)
                    writer.WriteString(context.GetLazyString(stats.LastQueryingTime.Value.GetDefaultRavenFormat(isUtc: true)));
                else
                    writer.WriteNull();
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(stats.LockMode)));
                writer.WriteString(context.GetLazyString(stats.LockMode.ToString()));
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(stats.Name)));
                writer.WriteString(context.GetLazyString(stats.Name));
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(stats.Priority)));
                writer.WriteString(context.GetLazyString(stats.Priority.ToString()));
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(stats.Type)));
                writer.WriteString(context.GetLazyString(stats.Type.ToString()));
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(stats.CreatedTimestamp)));
                writer.WriteString(context.GetLazyString(stats.CreatedTimestamp.GetDefaultRavenFormat(isUtc: true)));
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(stats.EntriesCount)));
                writer.WriteInteger(stats.EntriesCount);
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(stats.Id)));
                writer.WriteInteger(stats.Id);
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(stats.IndexingAttempts)));
                writer.WriteInteger(stats.IndexingAttempts);
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(stats.IndexingErrors)));
                writer.WriteInteger(stats.IndexingErrors);
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(stats.IndexingSuccesses)));
                writer.WriteInteger(stats.IndexingSuccesses);
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(stats.IsTestIndex)));
                writer.WriteBool(stats.IsTestIndex);

                writer.WriteEndObject();
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/indexes", "RESET")]
        public Task Reset()
        {
            var names = HttpContext.Request.Query["name"];
            if (names.Count != 1)
                throw new ArgumentException("Query string value 'name' must appear exactly once");
            if (string.IsNullOrWhiteSpace(names[0]))
                throw new ArgumentException("Query string value 'name' must have a non empty value");

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
            var names = HttpContext.Request.Query["name"];
            if (names.Count != 1)
                throw new ArgumentException("Query string value 'name' must appear exactly once");
            if (string.IsNullOrWhiteSpace(names[0]))
                throw new ArgumentException("Query string value 'name' must have a non empty value");

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
    }
}