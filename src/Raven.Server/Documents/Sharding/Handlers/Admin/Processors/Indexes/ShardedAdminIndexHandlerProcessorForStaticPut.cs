using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Admin.Processors.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Indexes;

internal class ShardedAdminIndexHandlerProcessorForStaticPut : AbstractAdminIndexHandlerProcessorForStaticPut<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedAdminIndexHandlerProcessorForStaticPut([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override AbstractIndexCreateController GetIndexCreateProcessor() => RequestHandler.DatabaseContext.Indexes.Create;

    protected override ValueTask HandleLegacyIndexesAsync()
    {
        throw new NotSupportedException("Legacy replication of indexes isn't supported in a sharded environment");
    }
}
