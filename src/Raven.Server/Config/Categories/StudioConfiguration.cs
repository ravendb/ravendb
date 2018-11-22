using System;
using System.ComponentModel;
using System.Linq;
using Microsoft.Extensions.Configuration;
using Raven.Server.Config.Attributes;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;

namespace Raven.Server.Config.Categories
{
    public class StudioConfiguration : ConfigurationCategory
    {
        [Description("The directory in which RavenDB will search the studio files, defaults to the base directory")]
        [DefaultValue(null)]
        [ConfigurationEntry("Studio.Path", ConfigurationEntryScope.ServerWideOnly)]
        public string Path { get; set; }
        
        [Description("Semicolon seperated list of notification names which will not be shown in the Studio. If not specified, all notifications are allowed. Example: \"SlowIO;Server_NewVersionAvailable;ClusterTopologyWarning\".")]
        [DefaultValue(null)]
        [ConfigurationEntry("Studio.Notifications.FilterOut", ConfigurationEntryScope.ServerWideOnly)]
        public string[] FilteredOutNotifications { get; set; }

        public override void Initialize(IConfigurationRoot settings, IConfigurationRoot serverWideSettings, ResourceType type, string resourceName)
        {
            base.Initialize(settings, serverWideSettings, type, resourceName);

            if (type != ResourceType.Server)
                return;

            ValidateFilteredOutNotificationNames();
        }

        private void ValidateFilteredOutNotificationNames()
        {
            if (FilteredOutNotifications == null)
                return;

            foreach (var notificationName in FilteredOutNotifications)
            {
                if (Enum.TryParse(typeof(NotificationType), notificationName, true, out _))
                    continue;

                if (Enum.TryParse(typeof(AlertType), notificationName, true, out _))
                    continue;

                if (Enum.TryParse(typeof(PerformanceHintType), notificationName, true, out _))
                    continue;

                throw new ArgumentException($"The notification name `{notificationName}` listed under the configuration '{RavenConfiguration.GetKey(x => x.Studio.FilteredOutNotifications)}' is unknown.");
            }
        }

        public bool ShouldFilterOut(Notification notification)
        {
            if (FilteredOutNotifications == null)
                return false;

            string name;

            // A user may choose to filter out a specific notification of either AlertRaised or PerformanceHint types
            // or they may filter out an entire notification type (e.g. DatabaseChanged, NotificationUpdated, etc.)
            switch (notification)
            {
                case AlertRaised alert:
                    name = alert.AlertType.ToString();
                    break;
                case PerformanceHint hint:
                    name = hint.HintType.ToString();
                    break;
                default:
                    name = notification.Type.ToString();
                    break;
            }
            
            return FilteredOutNotifications.Contains(name, StringComparer.OrdinalIgnoreCase);
        }
    }
}
