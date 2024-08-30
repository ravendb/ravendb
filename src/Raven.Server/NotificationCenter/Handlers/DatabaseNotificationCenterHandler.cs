using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Handlers.Processors;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter.Handlers
{
    public sealed class DatabaseNotificationCenterHandler : DatabaseRequestHandler
    {
        [Flags]
        private enum NotificationTypeParameter : short
        {
            None = 0,
            Alert = 1,
            PerformanceHint = 1 << 1,
        }

        private static readonly short SupportedFilterFlags = (short)(NotificationTypeParameter.Alert | NotificationTypeParameter.PerformanceHint);

        [RavenAction("/databases/*/notifications", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, SkipUsagesCount = true)]
        public async Task GetNotifications()
        {
            var postponed = GetBoolValueQueryString("postponed", required: false) ?? true;
            var type = GetStringQueryString("type", required: false);
            var start = GetIntValueQueryString("pageStart", required: false) ?? 0;
            var pageSize = GetIntValueQueryString("pageSize", required: false) ?? int.MaxValue;

            NotificationTypeParameter filter = NotificationTypeParameter.None;
            var shouldFilter = type != null;
            if (shouldFilter && (Enum.TryParse(type.AsSpan(), ignoreCase: true, out filter) == false || filter == NotificationTypeParameter.None || ((short)filter & ~SupportedFilterFlags) != 0))
            {
                var supportedNotificationTypeParameters = Enum.GetValues(typeof(NotificationTypeParameter))
                    .OfType<NotificationTypeParameter>()
                    .Where(x => x != NotificationTypeParameter.None)
                    .ToArray();

                throw new BadRequestException($"Accepted values for type parameter are: [{string.Join(", ", supportedNotificationTypeParameters)}]. Instead, got '{type}'. " +
                                              $"Type parameter is a flag, passing a list of types e.g. 'type=alert,performancehint' is also supported.");
            }

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            using (Database.NotificationCenter.GetStored(out var storedNotifications, postponed))
            {
                writer.WriteStartObject();

                var countQuery = pageSize == 0;
                var totalResults = 0;
                var isFirst = true;

                writer.WritePropertyName("Results");
                writer.WriteStartArray();
                foreach (var notification in storedNotifications)
                {
                    using (notification)
                    {
                        if (shouldFilter && notification.Json != null)
                        {
                            if (notification.Json.TryGet(nameof(Notification.Type), out string notificationType) == false
                                || Enum.TryParse(notificationType.AsSpan(), out NotificationType alertType) == false)
                                continue;

                            if (ShouldIncludeNotification(alertType) == false)
                                continue;
                        }

                        totalResults++;

                        if (start > 0)
                        {
                            start--;
                            continue;
                        }

                        if (pageSize == 0 && countQuery == false)
                            countQuery = true;

                        pageSize--;

                        if (countQuery)
                            continue;

                        if (isFirst == false)
                        {
                            writer.WriteComma();
                        }

                        writer.WriteObject(notification.Json);
                        isFirst = false;
                    }
                }

                writer.WriteEndArray();

                writer.WriteComma();
                writer.WritePropertyName("TotalResults");
                writer.WriteInteger(totalResults);

                writer.WriteEndObject();
            }

            bool ShouldIncludeNotification(in NotificationType notificationType)
            {
                return notificationType switch
                {
                    NotificationType.AlertRaised => filter.HasFlag(NotificationTypeParameter.Alert),
                    NotificationType.PerformanceHint => filter.HasFlag(NotificationTypeParameter.PerformanceHint),
                    _ => false
                };
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
