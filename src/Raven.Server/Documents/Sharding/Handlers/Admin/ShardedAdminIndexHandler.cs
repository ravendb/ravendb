using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Indexes;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands.Indexes;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Admin
{
    public class ShardedAdminIndexHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/indexes", "PUT")]
        public async Task Put()
        {
            var isReplicated = GetBoolValueQueryString("is-replicated", required: false);
            if (isReplicated.HasValue && isReplicated.Value)
            {
                throw new NotSupportedException("Legacy replication of indexes isn't supported in a sharded environment");
            }

            await AdminIndexHandler.PutInternal(new AdminIndexHandler.PutIndexParameters(this, validatedAsAdmin: true,
                ContextPool, DatabaseContext.DatabaseName, PutIndexTask, async args =>
                {
                    await Cluster.WaitForExecutionOfRaftCommandsAsync(args.Context, args.RaftIndexIds);
                    DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Critical, "After this method completes not all shards have the index");
                    await Task.Delay(TimeSpan.FromSeconds(5));
                }));
        }

        [RavenShardedAction("/databases/*/indexes", "PUT")]
        public async Task PutJavaScript()
        {
            await AdminIndexHandler.PutInternal(new AdminIndexHandler.PutIndexParameters(this, validatedAsAdmin: false,
                ContextPool, DatabaseContext.DatabaseName, PutIndexTask, async args =>
                {
                    await Cluster.WaitForExecutionOfRaftCommandsAsync(args.Context, args.RaftIndexIds);
                    DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Critical, "After this method completes not all shards have the index");
                    await Task.Delay(TimeSpan.FromSeconds(5));
                }));
        }

        [RavenShardedAction("/databases/*/admin/indexes/stop", "POST")]
        public async Task Stop()
        {
            using (var processor = new ShardedAdminIndexHandlerProcessorForStop(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/indexes/start", "POST")]
        public async Task Start()
        {
            using (var processor = new ShardedAdminIndexHandlerProcessorForStart(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/indexes/enable", "POST")]
        public async Task Enable()
        {
            using (var processor = new ShardedAdminIndexHandlerProcessorForState(IndexState.Normal, this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/indexes/disable", "POST")]
        public async Task Disable()
        {
            using (var processor = new ShardedAdminIndexHandlerProcessorForState(IndexState.Disabled, this))
                await processor.ExecuteAsync();
        }

        private async Task<long> PutIndexTask((IndexDefinition IndexDefinition, string RaftRequestId, string Source) args)
        {
            if (args.IndexDefinition == null)
                throw new ArgumentNullException(nameof(args.IndexDefinition));

            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal,
                "implement ValidateStaticIndex(definition)");
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal,
                "take the _documentDatabase.Configuration.Indexing.HistoryRevisionsNumber configuration from the database record");
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal,
                "take the _documentDatabase.Configuration.Indexing.StaticIndexDeploymentMode configuration from the database record");

            var command = new PutIndexCommand(
                args.IndexDefinition,
                DatabaseContext.DatabaseName,
                args.Source,
                ServerStore.Server.Time.GetUtcNow(),
                args.RaftRequestId,
                10,
                IndexDeploymentMode.Parallel
            );

            long index = 0;
            try
            {
                index = (await ServerStore.SendToLeaderAsync(command)).Index;
            }
            catch (Exception e)
            {
                IndexStore.ThrowIndexCreationException("static", args.IndexDefinition.Name, e, "the cluster is probably down", ServerStore);
            }

            return index;
        }
    }
}
