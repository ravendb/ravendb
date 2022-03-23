using System;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedDatabaseNotificationCenterHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/notification-center/watch", "GET")]
        public async Task Get()
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Marcin, DevelopmentHelper.Severity.Normal, "handle this");

            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                await Task.Delay(TimeSpan.FromHours(1));
            }
        }
    }
}
