using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Documents.Indexes;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands.Indexes;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers
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

            await AdminIndexHandler.PutInternal(new AdminIndexHandler.PutIndexParameters(this, validatedAsAdmin: true, 
                ContextPool, ShardedContext.DatabaseName, PutIndexTask, args => Cluster.WaitForExecutionOfRaftCommandsAsync(args.Context, args.RaftIndexIds)));
        }

        [RavenShardedAction("/databases/*/indexes", "PUT")]
        public async Task PutJavaScript()
        {
            await AdminIndexHandler.PutInternal(new AdminIndexHandler.PutIndexParameters(this, validatedAsAdmin: false,
                ContextPool, ShardedContext.DatabaseName, PutIndexTask, args => Cluster.WaitForExecutionOfRaftCommandsAsync(args.Context, args.RaftIndexIds)));
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
                ShardedContext.DatabaseName,
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
