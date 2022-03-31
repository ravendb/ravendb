using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Revisions;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Revisions;

internal class ShardedRevisionsHandlerProcessorForPostRevisionsConfiguration : AbstractRevisionsHandlerProcessorForPostRevisionsConfiguration<ShardedDatabaseRequestHandler>
{
    public ShardedRevisionsHandlerProcessorForPostRevisionsConfiguration([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override string GetDatabaseName() => RequestHandler.DatabaseContext.DatabaseName;

    protected override ValueTask WaitForIndexNotificationAsync(long index)
    {
        return RequestHandler.DatabaseContext.Cluster.WaitForExecutionOnShardsAsync(index);
    }
}
