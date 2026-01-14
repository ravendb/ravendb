using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Server.NotificationCenter.Notifications;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter.Handlers;

public class NotificationCenterHandlerHelper
{
    [Flags]
    private enum NotificationTypeParameter : short
    {
        None = 0,
        Alert = 1,
        PerformanceHint = 1 << 1,
    }
    
    private static readonly short SupportedFilterFlags = (short)(NotificationTypeParameter.Alert | NotificationTypeParameter.PerformanceHint);
    
    internal static async Task GetNotificationsFromStorageAsync(AbstractNotificationCenter notificationCenter, JsonOperationContext context, Stream responseBodyStream, bool postponed, string type, int start, int pageSize)
    {
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
        
        await using (var writer = new AsyncBlittableJsonTextWriter(context, responseBodyStream))
        using (notificationCenter.GetStored(out var storedNotifications, postponed))
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
}
