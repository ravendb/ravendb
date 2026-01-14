using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Handlers.Processors;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.NotificationCenter.Handlers
{
    public sealed class DatabaseNotificationCenterHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/notifications", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, SkipUsagesCount = true, IsDebugInformationEndpoint = true)]
        public async Task GetNotifications()
        {
            var postponed = GetBoolValueQueryString("postponed", required: false) ?? true;
            var type = GetStringQueryString("type", required: false);
            var start = GetIntValueQueryString("pageStart", required: false) ?? 0;
            var pageSize = GetIntValueQueryString("pageSize", required: false) ?? int.MaxValue;
            
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                await NotificationCenterHandlerHelper.GetNotificationsFromStorageAsync(Database.NotificationCenter, context, ResponseBodyStream(), postponed, type, start, pageSize);
            }
        }

        [RavenAction("/databases/*/notification-center/watch", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, SkipUsagesCount = true)]
        public async Task Watch()
        {
            using (var processor = new DatabaseNotificationCenterHandlerProcessorForWatch(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/notification-center/dismiss", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Dismiss()
        {
            using (var processor = new DatabaseNotificationCenterHandlerProcessorForDismiss(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/notification-center/postpone", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Postpone()
        {
            using (var processor = new DatabaseNotificationCenterHandlerProcessorForPostpone(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/notification-center/stats", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Stats()
        {
            using (var processor = new DatabaseNotificationCenterHandlerProcessorForStats(this))
                await processor.ExecuteAsync();
        }
    }
}
