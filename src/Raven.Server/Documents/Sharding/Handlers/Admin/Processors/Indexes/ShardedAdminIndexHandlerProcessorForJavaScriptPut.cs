using JetBrains.Annotations;
using Raven.Server.Config;
using Raven.Server.Documents.Handlers.Admin.Processors.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Indexes;

internal class ShardedAdminIndexHandlerProcessorForJavaScriptPut : AbstractAdminIndexHandlerProcessorForJavaScriptPut<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedAdminIndexHandlerProcessorForJavaScriptPut([NotNull] ShardedDatabaseRequestHandler requestHandler) 
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override string GetDatabaseName() => RequestHandler.DatabaseContext.DatabaseName;

    protected override AbstractIndexCreateProcessor GetIndexCreateProcessor() => RequestHandler.DatabaseContext.Indexes.Create;

    protected override RavenConfiguration GetDatabaseConfiguration() => RequestHandler.DatabaseContext.Configuration;
}
