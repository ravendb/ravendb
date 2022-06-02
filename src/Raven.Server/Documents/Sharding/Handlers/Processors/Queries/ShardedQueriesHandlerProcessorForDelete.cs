using System;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors.Queries;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Queries;

internal class ShardedQueriesHandlerProcessorForDelete : AbstractShardedOperationQueriesHandlerProcessor
{
    public ShardedQueriesHandlerProcessorForDelete([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.DatabaseContext.QueryMetadataCache)
    {
    }
    protected override HttpMethod OperationMethod => HttpMethod.Delete;

    protected override (Func<JsonOperationContext, int, RavenCommand<OperationIdResult>> CommandFactory, OperationType Type) GetOperation(IndexQueryServerSide query, long operationId, QueryOperationOptions options)
    {
        return ((c, shardNumber) => new DeleteByQueryOperation.DeleteByQueryCommand<BlittableJsonReaderObject>(DocumentConventions.DefaultForServer, query, options, operationId), OperationType.UpdateByQuery);
    }
}
