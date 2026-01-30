using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Operations;
using Raven.Server.Routing;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.Operations
{
    public sealed class OperationsServerHandler : ServerRequestHandler
    {
        [RavenAction("/admin/operations/next-operation-id", "GET", AuthorizationStatus.Operator)]
        public async Task GetNextOperationId()
        {
            var nextId = ServerStore.Operations.GetNextOperationId();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Id");
                    writer.WriteInteger(nextId);
                    writer.WriteEndObject();
                }
            }
        }

        [RavenAction("/admin/operations/kill", "POST", AuthorizationStatus.Operator)]
        public async Task Kill()
        {
            var id = GetLongQueryString("id");
            // ReSharper disable once PossibleInvalidOperationException
            await ServerStore.Operations.KillOperationAsync(id, CancellationToken.None);

            NoContentStatus();
        }

        [RavenAction("/admin/debug/operations/longest-running", "GET", AuthorizationStatus.Operator)]
        public async Task GetLongestRunningOperations()
        {
            var results = new Dictionary<OperationType, AbstractOperation>();

            FillOperations(results, ServerStore.Operations.GetActive());

            foreach (var database in GetLoadedDatabases())
            {
                if (TryGetActiveOperations(database, out var activeOperations) == false)
                    continue;

                FillOperations(results, activeOperations);
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, ConvertToJson(results));
                }
            }

            return;

            static bool TryGetActiveOperations(DatabasesLandlord.DatabaseSearchResult database, out ICollection<AbstractOperation> activeOperations)
            {
                try
                {
                    switch (database.DatabaseStatus)
                    {
                        case DatabasesLandlord.DatabaseSearchResult.Status.Database:
                            activeOperations = database.DatabaseTask.Result.Operations.GetActive();
                            break;
                        case DatabasesLandlord.DatabaseSearchResult.Status.Sharded:
                            activeOperations = database.DatabaseContext.Operations.GetActive();
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    return true;
                }
                catch
                {
                    // e.g. when database is being unloaded

                    activeOperations = null;
                    return false;
                }
            }

            static DynamicJsonValue ConvertToJson(Dictionary<OperationType, AbstractOperation> results)
            {
                var djv = new DynamicJsonValue();
                foreach (var kvp in results)
                {
                    var operation = kvp.Value;
                    var json = operation.Description.ToJson();
                    json[nameof(operation.Id)] = operation.Id;
                    json[nameof(operation.DatabaseName)] = operation.DatabaseName;

                    djv[kvp.Key.ToString()] = json;
                }

                return djv;
            }

            static void FillOperations(Dictionary<OperationType, AbstractOperation> results, ICollection<AbstractOperation> activeOperations)
            {
                foreach (var operation in activeOperations)
                {
                    var description = operation.Description;
                    if (description == null)
                        continue;

                    var startTime = description.StartTime;

                    if (results.TryGetValue(description.TaskType, out var maxOperation) == false || maxOperation.Description.StartTime > startTime)
                        results[description.TaskType] = operation;
                }
            }

            IEnumerable<DatabasesLandlord.DatabaseSearchResult> GetLoadedDatabases()
            {
                foreach (var kvp in ServerStore.DatabasesLandlord.DatabasesCache)
                {
                    var databaseTask = kvp.Value;

                    if (databaseTask == null || databaseTask.IsCompletedSuccessfully == false)
                        continue;

                    yield return new DatabasesLandlord.DatabaseSearchResult(DatabasesLandlord.DatabaseSearchResult.Status.Database, databaseTask, null);
                }

                foreach (var kvp in ServerStore.DatabasesLandlord.ShardedDatabasesCache)
                {
                    var databaseTask = kvp.Value;

                    if (databaseTask == null || databaseTask.IsCompletedSuccessfully == false)
                        continue;

                    yield return new DatabasesLandlord.DatabaseSearchResult(DatabasesLandlord.DatabaseSearchResult.Status.Sharded, null, databaseTask.Result);
                }
            }
        }

        [RavenAction("/operations/state", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task State()
        {
            var id = GetLongQueryString("id");
            // ReSharper disable once PossibleInvalidOperationException
            var operation = ServerStore.Operations.GetOperation(id);
            if (operation == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }

            if (operation.DatabaseName == null) // server level op
            {
                if (await IsOperatorAsync() == false)
                    return;
            }
            else if (await CanAccessDatabaseAsync(operation.DatabaseName, requireAdmin: false, requireWrite: false) == false)
            {
                return;
            }

            var state = operation.State;

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, state.ToJson());
                }
            }
        }
    }
}
