using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Exceptions;
using Raven.Server.NotificationCenter.Notifications;

namespace Raven.Server.NotificationCenter.Handlers.Processors;

public static class NotificationCenterHelper
{
    [Flags]
    internal enum NotificationTypeParameter : short
    {
        None = 0,
        Alert = 1,
        PerformanceHint = 1 << 1,
    }

    private const short SupportedFilterFlags = (short)(NotificationTypeParameter.Alert | NotificationTypeParameter.PerformanceHint);

    internal static NotificationTypeParameter GetAndEnsureValidTypeParameters(string type)
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

        return filter;
    }

    internal static IEnumerable<NotificationTableValue> FilterNotifications(IEnumerable<NotificationTableValue> notifications, NotificationTypeParameter filter)
    {
        foreach (var notification in notifications)
        {
            var shouldInclude = false;

            if (notification.Json != null)
            {
                if (notification.Json.TryGet(nameof(Notification.Type), out string notificationType)
                    && Enum.TryParse(notificationType.AsSpan(), out NotificationType alertType))
                {
                    if (ShouldIncludeNotification(alertType))
                        shouldInclude = true;
                }
            }

            if (shouldInclude)
            {
                yield return notification;
            }
            else
            {
                notification.Dispose();
            }
        }

        yield break;

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
