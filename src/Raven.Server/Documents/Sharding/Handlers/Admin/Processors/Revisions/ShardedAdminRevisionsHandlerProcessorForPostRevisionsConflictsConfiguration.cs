using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Admin.Processors.Revisions;

namespace Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Revisions
{
    internal class ShardedAdminRevisionsHandlerProcessorForPostRevisionsConflictsConfiguration : AbstractAdminRevisionsHandlerProcessorForPostRevisionsConflictsConfiguration<ShardedDatabaseRequestHandler>
    {
        public ShardedAdminRevisionsHandlerProcessorForPostRevisionsConflictsConfiguration([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override string GetDatabaseName()
        {
            return RequestHandler.DatabaseContext.DatabaseName;
        }

        protected override async ValueTask WaitForIndexNotificationAsync(long index)
        {
            await RequestHandler.DatabaseContext.Cluster.WaitForExecutionOnAllNodesAsync(index);
        }
    }
}
