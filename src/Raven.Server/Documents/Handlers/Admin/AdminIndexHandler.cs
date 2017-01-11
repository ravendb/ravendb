using System;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Exceptions;
using Raven.Client.Data.Indexes;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminIndexHandler : AdminDatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/indexes/compact", "POST")]
        public Task Compact()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            var index = Database.IndexStore.GetIndex(name);
            if (index == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return Task.CompletedTask;
            }

            var token = CreateOperationToken();
            var operationId = Database.Operations.GetNextOperationId();

            Database.Operations.AddOperation(
                "Compact index: " + index.Name,
                DatabaseOperations.PendingOperationType.IndexCompact,
                onProgress => Task.Factory.StartNew(() => index.Compact(onProgress), token.Token), operationId, token);

            JsonOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteOperationId(context, operationId);
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/admin/indexes/stop", "POST")]
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

        [RavenAction("/databases/*/admin/indexes/start", "POST")]
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

        private void ToogleIndexesStatus(bool enableRequest)
        {
            var dbName = Database.Name;
            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            using (var tx = context.OpenWriteTransaction())
            {
                var dbId = $"db/{dbName}";
                var dbConfigDoc = ServerStore.Read(context, dbId);
                BlittableJsonReaderObject settings;
                dbConfigDoc.TryGet("Settings", out settings);
                if (settings == null)
                    return;

                var disable = Database.Configuration.Indexing.Disabled;
                if (disable != enableRequest)
                {
                    var state = disable ? "disabled" : "enabled";
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["Database Name"] = dbName,
                        ["Success"] = false,
                        ["Indexing Disabled"] = disable,
                        ["Reason"] = $"Indexing are already {state}"
                    });

                    tx.Commit();
                    writer.WriteEndArray();
                    return;
                }

                if (disable)
                {
                    settings.Modifications = new DynamicJsonValue(settings);
                    settings.Modifications.Remove("Raven/Indexing/Disable");
                }
                else
                {
                    settings.Modifications = new DynamicJsonValue(settings)
                    {
                        ["Raven/Indexing/Disable"] = true
                    };
                }

                var newDoc2 = context.ReadObject(dbConfigDoc, dbId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                ServerStore.Write(context, dbId, newDoc2);

                context.Write(writer, new DynamicJsonValue
                {
                    ["Database Name"] = dbName,
                    ["Success"] = true,
                    ["Indexing Disabled"] = !disable
                });

                tx.Commit();
                writer.WriteEndArray();
            }

            ServerStore.DatabasesLandlord.UnloadAndLock(dbName, () =>
            {
                //empty by design
            });
        }

        [RavenAction("/databases/*/admin/indexes/enable", "POST")]
        public Task Enable()
        {
            var name = GetStringQueryString("name", required: false);
            if (string.IsNullOrEmpty(name))
            {
                ToogleIndexesStatus(enableRequest: true);
                return Task.CompletedTask;
            }

            var index = Database.IndexStore.GetIndex(name);
            if (index == null)
                IndexDoesNotExistsException.ThrowFor(name);

            index.Enable();

            HttpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/admin/indexes/disable", "POST")]
        public Task Disable()
        {
            var name = GetStringQueryString("name", required: false);
            if (string.IsNullOrEmpty(name))
            {
                ToogleIndexesStatus(enableRequest: false);
                return Task.CompletedTask;
            }

            var index = Database.IndexStore.GetIndex(name);
            if (index == null)
                IndexDoesNotExistsException.ThrowFor(name);

            index.Disable();

            HttpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;
            return Task.CompletedTask;
        }
    }
}