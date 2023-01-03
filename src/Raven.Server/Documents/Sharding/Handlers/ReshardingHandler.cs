using System;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Database;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ReshardingHandler : ServerRequestHandler
    {
        [RavenAction("/admin/resharding", "GET", AuthorizationStatus.Operator)]
        public async Task StartResharding()
        {
            var database = GetStringQueryString("database");
            var bucket = GetIntValueQueryString("bucket").Value;
            var from = GetIntValueQueryString("from", required: false);
            var to = GetIntValueQueryString("to").Value;
            var raftId = GetRaftRequestIdFromQuery();

            using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            using (var raw = ServerStore.Cluster.ReadRawDatabaseRecord(context, database))
            {
                if (raw == null)
                    DatabaseDoesNotExistException.Throw(database);

                if (raw.IsSharded == false)
                    throw new InvalidOperationException($"{database} is not sharded");

                if (from.HasValue == false)
                {
                    var config = raw.Sharding;
                    from = ShardHelper.GetShardNumber(config.ShardBucketRanges, bucket);
                }
            }

            await ServerStore.Sharding.StartBucketMigration(database, bucket, from.Value, to, raftId);
        }
    }
}
