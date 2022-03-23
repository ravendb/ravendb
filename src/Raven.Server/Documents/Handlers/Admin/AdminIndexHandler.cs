using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Handlers.Admin.Processors.Indexes;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminIndexHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/indexes", "PUT", AuthorizationStatus.DatabaseAdmin, DisableOnCpuCreditsExhaustion = true)]
        public async Task Put()
        {
            using (var processor = new AdminIndexHandlerProcessorForStaticPut(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/indexes", "PUT", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task PutJavaScript()
        {
            using (var processor = new AdminIndexHandlerProcessorForJavaScriptPut(this))
                await processor.ExecuteAsync();
        }

        internal static async Task PutInternal(PutIndexParameters parameters)
        {
            using (parameters.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var createdIndexes = new List<PutIndexResult>();
                var raftIndexIds = new List<long>();

                var input = await context.ReadForMemoryAsync(parameters.RequestHandler.RequestBodyStream(), "Indexes");
                if (input.TryGet("Indexes", out BlittableJsonReaderArray indexes) == false)
                    ThrowRequiredPropertyNameInRequest("Indexes");

                var raftRequestId = parameters.RequestHandler.GetRaftRequestIdFromQuery();
                foreach (BlittableJsonReaderObject indexToAdd in indexes)
                {
                    var indexDefinition = JsonDeserializationServer.IndexDefinition(indexToAdd);
                    indexDefinition.Name = indexDefinition.Name?.Trim();

                    var source = IsLocalRequest(parameters.RequestHandler.HttpContext) ? Environment.MachineName : parameters.RequestHandler.HttpContext.Connection.RemoteIpAddress.ToString();

                    if (LoggingSource.AuditLog.IsInfoEnabled)
                    {
                        var clientCert = parameters.RequestHandler.GetCurrentCertificate();

                        var auditLog = LoggingSource.AuditLog.GetLogger(parameters.DatabaseName, "Audit");
                        auditLog.Info($"Index {indexDefinition.Name} PUT by {clientCert?.Subject} {clientCert?.Thumbprint} with definition: {indexToAdd} from {source} at {DateTime.UtcNow}");
                    }

                    if (indexDefinition.Maps == null || indexDefinition.Maps.Count == 0)
                        throw new ArgumentException("Index must have a 'Maps' fields");

                    indexDefinition.Type = indexDefinition.DetectStaticIndexType();

                    // C# index using a non-admin endpoint
                    if (indexDefinition.Type.IsJavaScript() == false && parameters.ValidatedAsAdmin == false)
                    {
                        throw new UnauthorizedAccessException($"Index {indexDefinition.Name} is a C# index but was sent through a non-admin endpoint using REST api, this is not allowed.");
                    }

                    if (indexDefinition.Name.StartsWith(Constants.Documents.Indexing.SideBySideIndexNamePrefix, StringComparison.OrdinalIgnoreCase))
                    {
                        throw new ArgumentException(
                            $"Index name must not start with '{Constants.Documents.Indexing.SideBySideIndexNamePrefix}'. Provided index name: '{indexDefinition.Name}'");
                    }

                    var index = await parameters.PutIndexTask(indexDefinition, $"{raftRequestId}/{indexDefinition.Name}", source);
                    raftIndexIds.Add(index);

                    createdIndexes.Add(new PutIndexResult
                    {
                        Index = indexDefinition.Name,
                        RaftCommandIndex = index
                    });
                }

                if (TrafficWatchManager.HasRegisteredClients)
                    parameters.RequestHandler.AddStringToHttpContext(indexes.ToString(), TrafficWatchChangeType.Index);

                if (parameters.WaitForIndexNotification != null)
                    await parameters.WaitForIndexNotification((context, raftIndexIds));

                parameters.RequestHandler.HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, parameters.RequestHandler.ResponseBodyStream()))
                {
                    writer.WritePutIndexResponse(context, createdIndexes);
                }
            }
        }

        internal class PutIndexParameters
        {
            public PutIndexParameters(RequestHandler requestHandler, bool validatedAsAdmin, TransactionContextPool contextPool,
                string databaseName, Func<IndexDefinition, string, string, ValueTask<long>> putIndexTask,
                Func<(JsonOperationContext Context, List<long> RaftIndexIds), Task> waitForIndexNotification = null)
            {
                RequestHandler = requestHandler;
                ValidatedAsAdmin = validatedAsAdmin;
                ContextPool = contextPool;
                DatabaseName = databaseName;
                PutIndexTask = putIndexTask;
                WaitForIndexNotification = waitForIndexNotification;
            }

            public RequestHandler RequestHandler { get; }

            public bool ValidatedAsAdmin { get; }

            public TransactionContextPool ContextPool { get; }

            public string DatabaseName { get; }

            public Func<IndexDefinition, string, string, ValueTask<long>> PutIndexTask { get; }

            public Func<(JsonOperationContext, List<long>), Task> WaitForIndexNotification { get; }
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
        public async Task Stop()
        {
            using (var processor = new AdminIndexHandlerProcessorForStop(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/indexes/start", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task Start()
        {
            using (var processor = new AdminIndexHandlerProcessorForStart(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/indexes/enable", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task Enable()
        {
            using (var processor = new AdminIndexHandlerProcessorForState(IndexState.Normal, this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/indexes/disable", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task Disable()
        {
            using (var processor = new AdminIndexHandlerProcessorForState(IndexState.Disabled, this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/indexes/dump", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task Dump()
        {
            var name = GetStringQueryString("name");
            var path = GetStringQueryString("path");
            var index = Database.IndexStore.GetIndex(name);
            if (index == null)
            {
                IndexDoesNotExistException.ThrowFor(name);
                return; //never hit
            }

            var operationId = Database.Operations.GetNextOperationId();
            var token = CreateTimeLimitedQueryOperationToken();

            _ = Database.Operations.AddOperation(
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
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
            }
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
