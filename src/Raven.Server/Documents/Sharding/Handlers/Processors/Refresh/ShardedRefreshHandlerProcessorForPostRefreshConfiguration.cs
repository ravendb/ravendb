using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Refresh;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Refresh;

internal class ShardedRefreshHandlerProcessorForPostRefreshConfiguration : AbstractRefreshHandlerProcessorForPostRefreshConfiguration<ShardedDatabaseRequestHandler>
{
    public ShardedRefreshHandlerProcessorForPostRefreshConfiguration([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override string GetDatabaseName() => RequestHandler.DatabaseContext.DatabaseName;

    protected override ValueTask WaitForIndexNotificationAsync(long index) => RequestHandler.DatabaseContext.Cluster.WaitForExecutionOfRaftCommandsAsync(index);
}
