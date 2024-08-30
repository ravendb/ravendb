using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter.Handlers
{
    public class DatabaseNotificationCenterHandler : DatabaseRequestHandler
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
        public async Task Get()
        {
            try
            {
                using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
                {
                    using (var writer = new NotificationCenterWebSocketWriter(webSocket, Database.NotificationCenter, ContextPool, Database.DatabaseShutdown))
                    {
                        using (Database.NotificationCenter.GetStored(out IEnumerable<NotificationTableValue> storedNotifications, postponed: false))
                        {
                            foreach (var alert in storedNotifications)
                            {
                                using (alert)
                                    await writer.WriteToWebSocket(alert.Json);
                            }
                        }

                        foreach (var operation in Database.Operations.GetActive().OrderBy(x => x.Description.StartTime))
                        {
                            var action = OperationChanged.Create(Database.Name, operation.Id, operation.Description, operation.State, operation.Killable);

                            await writer.WriteToWebSocket(action.ToJson());
                        }
                        writer.AfterTrackActionsRegistration = ServerStore.NotifyAboutClusterTopologyAndConnectivityChanges;
                        await writer.WriteNotifications(null);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // disposing
            }
            catch (ObjectDisposedException)
            {
                // disposing
            }
        }

        [RavenAction("/databases/*/notification-center/dismiss", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public Task DismissPost()
        {
            var id = GetStringQueryString("id");
            var forever = GetBoolValueQueryString("forever", required: false);

            if (forever == true)
                Database.NotificationCenter.Postpone(id, DateTime.MaxValue);
            else
                Database.NotificationCenter.Dismiss(id);

            return NoContent();
        }

        [RavenAction("/databases/*/notification-center/postpone", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public Task PostponePost()
        {
            var id = GetStringQueryString("id");
            var timeInSec = GetLongQueryString("timeInSec");

            var until = timeInSec == 0 ? DateTime.MaxValue : SystemTime.UtcNow.Add(TimeSpan.FromSeconds(timeInSec));
            Database.NotificationCenter.Postpone(id, until);

            return NoContent();
        }
    }
}
