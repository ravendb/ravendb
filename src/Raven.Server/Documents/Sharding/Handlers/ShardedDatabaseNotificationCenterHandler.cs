using System;
using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Notifications;
using Raven.Server.Routing;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedDatabaseNotificationCenterHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/notification-center/watch", "GET")]
        public async Task Get()
        {
            using (var processor = new ShardedDatabaseNotificationCenterHandlerProcessorForGet(this))
                await processor.ExecuteAsync();
        }
    }
}
