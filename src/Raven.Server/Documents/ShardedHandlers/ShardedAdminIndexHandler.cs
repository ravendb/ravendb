using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Sharding;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands.Indexes;

namespace Raven.Server.Documents.ShardedHandlers
{
    public class ShardedAdminIndexHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/indexes", "PUT")]
        public async Task Put()
        {
            var isReplicated = GetBoolValueQueryString("is-replicated", required: false);
            if (isReplicated.HasValue && isReplicated.Value)
            {
                throw new NotSupportedException("Legacy replication of indexes isn't supported in a sharded environment");
            }

            await AdminIndexHandler.PutInternal(new AdminIndexHandler.PutIndexParameters
            {
                RequestHandler = this,
                ContextPool = ContextPool,
                DatabaseName = ShardedContext.DatabaseName,
                ValidatedAsAdmin = true,
                PutIndexTask = PutIndexTask,
                WaitForIndexNotification = WaitForExecutionOfDatabaseCommands
            });
        }

        [RavenShardedAction("/databases/*/indexes", "PUT")]
        public async Task PutJavaScript()
        {
            await AdminIndexHandler.PutInternal(new AdminIndexHandler.PutIndexParameters
            {
                RequestHandler = this,
                ContextPool = ContextPool,
                DatabaseName = ShardedContext.DatabaseName,
                ValidatedAsAdmin = false,
                PutIndexTask = PutIndexTask,
                WaitForIndexNotification = WaitForExecutionOfDatabaseCommands
            });
        }

        private async Task<long> PutIndexTask(IndexDefinition indexDefinition, string raftRequestId, string source = null)
        {
            if (indexDefinition == null)
                throw new ArgumentNullException(nameof(indexDefinition));

            //TODO: ValidateStaticIndex(definition);

            var command = new PutIndexCommand(
                indexDefinition,
                ShardedContext.DatabaseName,
                source,
                ServerStore.Server.Time.GetUtcNow(),
                raftRequestId,
                10,//TODO: _documentDatabase.Configuration.Indexing.HistoryRevisionsNumber,
                IndexDeploymentMode.Parallel//TODO: _documentDatabase.Configuration.Indexing.StaticIndexDeploymentMode
            );

            long index = 0;
            try
            {
                index = (await ServerStore.SendToLeaderAsync(command)).Index;
            }
            catch (Exception e)
            {
                IndexStore.ThrowIndexCreationException("static", indexDefinition.Name, e, "the cluster is probably down", ServerStore);
            }

            return index;
        }
    }
}
