using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Migration;
using Raven.Server.TrafficWatch;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using DatabaseSmuggler = Raven.Server.Smuggler.Documents.DatabaseSmuggler;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminIndexHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/indexes", "PUT", AuthorizationStatus.DatabaseAdmin, DisableOnCpuCreditsExhaustion = true)]
        public async Task Put()
        {
            await PutInternal(validatedAsAdmin: true);
        }

        [RavenAction("/databases/*/indexes", "PUT", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public async Task PutJavaScript()
        {
            await PutInternal(validatedAsAdmin: false);
        }

        private async Task PutInternal(bool validatedAsAdmin)
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var createdIndexes = new List<(string Name, long RaftIndex)>();

                var isReplicatedQueryString = GetStringQueryString("is-replicated", required: false);
                if (isReplicatedQueryString != null && bool.TryParse(isReplicatedQueryString, out var result) && result)
                {
                    HandleLegacyIndexes();
                    return;
                }

                var input = await context.ReadForMemoryAsync(RequestBodyStream(), "Indexes");
                if (input.TryGet("Indexes", out BlittableJsonReaderArray indexes) == false)
                    ThrowRequiredPropertyNameInRequest("Indexes");
                var raftRequestId = GetRaftRequestIdFromQuery();
                foreach (BlittableJsonReaderObject indexToAdd in indexes)
                {
                    var indexDefinition = JsonDeserializationServer.IndexDefinition(indexToAdd);
                    indexDefinition.Name = indexDefinition.Name?.Trim();

                    var source = IsLocalRequest(HttpContext) ? Environment.MachineName : HttpContext.Connection.RemoteIpAddress.ToString();

                    if (LoggingSource.AuditLog.IsInfoEnabled)
                    {
                        var clientCert = GetCurrentCertificate();

                        var auditLog = LoggingSource.AuditLog.GetLogger(Database.Name, "Audit");
                        auditLog.Info($"Index {indexDefinition.Name} PUT by {clientCert?.Subject} {clientCert?.Thumbprint} with definition: {indexToAdd} from {source} at {DateTime.UtcNow}");
                    }

                    if (indexDefinition.Maps == null || indexDefinition.Maps.Count == 0)
                        throw new ArgumentException("Index must have a 'Maps' fields");

                    indexDefinition.Type = indexDefinition.DetectStaticIndexType();

                    // C# index using a non-admin endpoint
                    if (indexDefinition.Type.IsJavaScript() == false && validatedAsAdmin == false)
                    {
                        throw new UnauthorizedAccessException($"Index {indexDefinition.Name} is a C# index but was sent through a non-admin endpoint using REST api, this is not allowed.");
                    }

                    if (indexDefinition.Name.StartsWith(Constants.Documents.Indexing.SideBySideIndexNamePrefix, StringComparison.OrdinalIgnoreCase))
                    {
                        throw new ArgumentException(
                            $"Index name must not start with '{Constants.Documents.Indexing.SideBySideIndexNamePrefix}'. Provided index name: '{indexDefinition.Name}'");
                    }

                    var index = await Database.IndexStore.CreateIndexInternal(indexDefinition, $"{raftRequestId}/{indexDefinition.Name}", source);

                    createdIndexes.Add((indexDefinition.Name, index));
                }
                if (TrafficWatchManager.HasRegisteredClients)
                    AddStringToHttpContext(indexes.ToString(), TrafficWatchChangeType.Index);


                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WriteArray(context, "Results", createdIndexes, (w, c, index) =>
                    {
                        w.WriteStartObject();
                        w.WritePropertyName(nameof(PutIndexResult.Index));
                        w.WriteString(index.Name);
                        w.WriteComma();
                        w.WritePropertyName(nameof(PutIndexResult.RaftCommandIndex));
                        w.WriteInteger(index.RaftIndex);
                        w.WriteEndObject();
                    });

                    writer.WriteEndObject();
                }
            }
        }

        private void HandleLegacyIndexes()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var stream = new ArrayStream(RequestBodyStream(), nameof(DatabaseItemType.Indexes)))
            using (var source = new StreamSource(stream, context, Database))
            {
                var destination = new DatabaseDestination(Database);
                var options = new DatabaseSmugglerOptionsServerSide
                {
                    OperateOnTypes = DatabaseItemType.Indexes
                };

                var smuggler = new DatabaseSmuggler(Database, source, destination, Database.Time, options);
                smuggler.Execute();
            }
        }

        public static bool IsLocalRequest(HttpContext context)
        {
            if (context.Connection.RemoteIpAddress == null && context.Connection.LocalIpAddress == null)
            {
                return true;
            }
            if (context.Connection.RemoteIpAddress.Equals(context.Connection.LocalIpAddress))
            {
                return true;
            }
            if (IPAddress.IsLoopback(context.Connection.RemoteIpAddress))
            {
                return true;
            }
            return false;
        }

        [RavenAction("/databases/*/admin/indexes/stop", "POST", AuthorizationStatus.DatabaseAdmin)]
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

        [RavenAction("/databases/*/admin/indexes/start", "POST", AuthorizationStatus.DatabaseAdmin)]
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

        [RavenAction("/databases/*/admin/indexes/enable", "POST", AuthorizationStatus.DatabaseAdmin)]
        public Task Enable()
        {
            var name = GetStringQueryString("name");
            var index = Database.IndexStore.GetIndex(name);
            if (index == null)
                IndexDoesNotExistException.ThrowFor(name);

            index.Enable();

            return NoContent();
        }

        [RavenAction("/databases/*/admin/indexes/disable", "POST", AuthorizationStatus.DatabaseAdmin)]
        public Task Disable()
        {
            var name = GetStringQueryString("name");
            var index = Database.IndexStore.GetIndex(name);
            if (index == null)
                IndexDoesNotExistException.ThrowFor(name);

            index.Disable();

            return NoContent();
        }

        [RavenAction("/databases/*/admin/indexes/dump", "POST", AuthorizationStatus.DatabaseAdmin)]
        public Task Dump()
        {
            var name = GetStringQueryString("name");
            var path = GetStringQueryString("path");
            var index = Database.IndexStore.GetIndex(name);
            if (index == null)
            {
                IndexDoesNotExistException.ThrowFor(name);
                return null;//never hit
            }

            var operationId = Database.Operations.GetNextOperationId();
            var token = CreateTimeLimitedQueryOperationToken();

            Database.Operations.AddOperation(
                Database,
                "Dump index " + name + " to " + path,
                Operations.Operations.OperationType.DumpRawIndexData,
                onProgress =>
                {
                    var totalFiles = index.Dump(path, onProgress);
                    return Task.FromResult((IOperationResult)new DumpIndexResult
                    {
                        Message = $"Dumped {totalFiles} files from {name}",
                    });
                }, operationId, token: token);

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
            }

            return Task.CompletedTask;
        }

        public class DumpIndexResult : IOperationResult
        {
            public string Message { get; set; }
            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue(GetType())
                {
                    [nameof(Message)] = Message,
                };
            }

            public bool ShouldPersist => false;
        }

        public class DumpIndexProgress : IOperationProgress
        {
            public int ProcessedFiles { get; set; }
            public int TotalFiles { get; set; }
            public string Message { get; set; }
            public long CurrentFileSizeInBytes { get; internal set; }
            public long CurrentFileCopiedBytes { get; internal set; }

            public virtual DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue(GetType())
                {
                    [nameof(ProcessedFiles)] = ProcessedFiles,
                    [nameof(TotalFiles)] = TotalFiles,
                    [nameof(Message)] = Message,
                    [nameof(CurrentFileSizeInBytes)] = CurrentFileSizeInBytes,
                    [nameof(CurrentFileCopiedBytes)] = CurrentFileCopiedBytes
                };
            }
        }
    }
}
