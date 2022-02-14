using System;
using System.Threading.Tasks;
using Raven.Server.Documents.Sharding;
using Raven.Server.Routing;
using Sparrow.Utils;

namespace Raven.Server.Documents.ShardedHandlers
{
    public class ShardedDatabaseNotificationCenterHandler : ShardedRequestHandler
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
