using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Notifications;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedDatabaseNotificationCenterHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/notification-center/watch", "GET")]
        public async Task Get()
        {
            using (var processor = new ShardedDatabaseNotificationCenterHandlerProcessorForWatch(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/notification-center/dismiss", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Dismiss()
        {
            using (var processor = new ShardedDatabaseNotificationCenterHandlerProcessorForDismiss(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/notification-center/postpone", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Postpone()
        {
            using (var processor = new ShardedDatabaseNotificationCenterHandlerProcessorForPostpone(this))
                await processor.ExecuteAsync();
        }
    }
}
