using System;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Exceptions.Indexes;
using Raven.Server.Documents.Operations;
using Raven.Server.Json;
using Raven.Server.Routing;
using Sparrow.Json;

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
                return NoContent();

            var token = CreateOperationToken();
            var operationId = Database.Operations.GetNextOperationId();

            Database.Operations.AddOperation(
                "Compact index: " + index.Name,
                DatabaseOperations.OperationType.IndexCompact,
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
                Database.IndexStore.StopIndexing();
                return NoContent();
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

            return NoContent();
        }

        [RavenAction("/databases/*/admin/indexes/start", "POST")]
        public Task Start()
        {
            var types = HttpContext.Request.Query["type"];
            var names = HttpContext.Request.Query["name"];
            if (types.Count == 0 && names.Count == 0)
            {
                Database.IndexStore.StartIndexing();

                return NoContent();
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

            return NoContent();
        }

        [RavenAction("/databases/*/admin/indexes/enable", "POST")]
        public Task Enable()
        {
            var name = GetStringQueryString("name");
            var index = Database.IndexStore.GetIndex(name);
            if (index == null)
                IndexDoesNotExistException.ThrowFor(name);

            index.Enable();

            return NoContent();
        }

        [RavenAction("/databases/*/admin/indexes/disable", "POST")]
        public Task Disable()
        {
            var name = GetStringQueryString("name");
            var index = Database.IndexStore.GetIndex(name);
            if (index == null)
                IndexDoesNotExistException.ThrowFor(name);

            index.Disable();

            return NoContent();
        }
    }
}