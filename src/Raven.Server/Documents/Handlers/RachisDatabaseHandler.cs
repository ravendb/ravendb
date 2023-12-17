using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Rachis;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers
{
    public class RachisDatabaseHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/rachis/wait-for-index-notification", "Post", AuthorizationStatus.DatabaseAdmin)]
        public async Task WaitForDatabaseNotification()
        {
            var index = GetLongQueryString("index");
            using (var cts = new CancellationTokenSource(Server.Configuration.Cluster.OperationTimeout.AsTimeSpan))
            {
                await ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, index, cts.Token);
                await Database.RachisLogIndexNotifications.WaitForIndexNotification(index, cts.Token);
            }
        }
    }
}
