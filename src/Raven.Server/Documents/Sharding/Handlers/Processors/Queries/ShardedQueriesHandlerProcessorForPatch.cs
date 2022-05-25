using System;
using System.Net.Http;
using JetBrains.Annotations;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Http;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Queries;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Queries;

internal class ShardedQueriesHandlerProcessorForPatch : AbstractShardedOperationQueriesHandlerProcessor
{
    public ShardedQueriesHandlerProcessorForPatch([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.DatabaseContext.QueryMetadataCache)
    {
    }
    protected override HttpMethod OperationMethod => HttpMethod.Patch;

    protected override (Func<JsonOperationContext, RavenCommand<OperationIdResult>> CommandFactory, OperationType Type) GetOperation(IndexQueryServerSide query, long operationId, QueryOperationOptions options)
    {
        return (c => new PatchByQueryOperation.PatchByQueryCommand<BlittableJsonReaderObject>(DocumentConventions.DefaultForServer, c, query, options, operationId), OperationType.UpdateByQuery);
    }
}
