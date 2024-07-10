using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
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

                var clientCert = RequestHandler.GetCurrentCertificate();

                string source = clientCert != null
                    ? $"{RequestHandler.RequestIp} | {clientCert.Subject} [{clientCert.Thumbprint}]"
                    : $"{RequestHandler.RequestIp}";

                if (LoggingSource.AuditLog.IsInfoEnabled)
                {
                    RequestHandler.LogAuditFor(RequestHandler.DatabaseName, "PUT", $"Index '{indexDefinition.Name}' with definition: {indexToAdd}");
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
}
