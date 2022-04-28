using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.TrafficWatch;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Indexes;

internal abstract class AbstractAdminIndexHandlerProcessorForPut<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    private readonly bool _validatedAsAdmin;

    protected AbstractAdminIndexHandlerProcessorForPut([NotNull] TRequestHandler requestHandler, bool validatedAsAdmin)
        : base(requestHandler)
    {
        _validatedAsAdmin = validatedAsAdmin;
    }

    protected abstract AbstractIndexCreateController GetIndexCreateProcessor();

    public override async ValueTask ExecuteAsync()
    {
        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            var createdIndexes = new List<PutIndexResult>();

            var input = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "Indexes");
            if (input.TryGet("Indexes", out BlittableJsonReaderArray indexes) == false)
                Web.RequestHandler.ThrowRequiredPropertyNameInRequest("Indexes");

            var raftRequestId = RequestHandler.GetRaftRequestIdFromQuery();
            foreach (BlittableJsonReaderObject indexToAdd in indexes)
            {
                var indexDefinition = JsonDeserializationServer.IndexDefinition(indexToAdd);
                indexDefinition.Name = indexDefinition.Name?.Trim();

                var source = IsLocalRequest(RequestHandler.HttpContext) ? Environment.MachineName : RequestHandler.HttpContext.Connection.RemoteIpAddress.ToString();

                if (LoggingSource.AuditLog.IsInfoEnabled)
                {
                    var clientCert = RequestHandler.GetCurrentCertificate();

                    var auditLog = LoggingSource.AuditLog.GetLogger(RequestHandler.DatabaseName, "Audit");
                    auditLog.Info($"Index {indexDefinition.Name} PUT by {clientCert?.Subject} {clientCert?.Thumbprint} with definition: {indexToAdd} from {source} at {DateTime.UtcNow}");
                }

                if (indexDefinition.Maps == null || indexDefinition.Maps.Count == 0)
                    throw new ArgumentException("Index must have a 'Maps' fields");

                indexDefinition.Type = indexDefinition.DetectStaticIndexType();

                // C# index using a non-admin endpoint
                if (indexDefinition.Type.IsJavaScript() == false && _validatedAsAdmin == false)
                {
                    throw new UnauthorizedAccessException($"Index {indexDefinition.Name} is a C# index but was sent through a non-admin endpoint using REST api, this is not allowed.");
                }

                if (indexDefinition.Name.StartsWith(Constants.Documents.Indexing.SideBySideIndexNamePrefix, StringComparison.OrdinalIgnoreCase))
                {
                    throw new ArgumentException(
                        $"Index name must not start with '{Constants.Documents.Indexing.SideBySideIndexNamePrefix}'. Provided index name: '{indexDefinition.Name}'");
                }

                var processor = GetIndexCreateProcessor();

                var index = await processor.CreateIndexAsync(indexDefinition, $"{raftRequestId}/{indexDefinition.Name}", source);

                createdIndexes.Add(new PutIndexResult
                {
                    Index = indexDefinition.Name,
                    RaftCommandIndex = index
                });
            }

            if (TrafficWatchManager.HasRegisteredClients)
                RequestHandler.AddStringToHttpContext(indexes.ToString(), TrafficWatchChangeType.Index);

            RequestHandler.HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WritePutIndexResponse(context, createdIndexes);
            }
        }
    }

    private static bool IsLocalRequest(HttpContext context)
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
}
