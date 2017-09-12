using Raven.Server.Commercial;
using Raven.Server.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    internal class LicenseLimitWarning : INotificationDetails
    {
        private LicenseLimitWarning()
        {
            
        }

        private LicenseLimitWarning(LicenseLimit licenseLimit)
        {
            Type = licenseLimit.Type;
            Message = licenseLimit.Message;
        }

        public LimitType Type { get; set; }

        public string Message { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(Type)] = Type,
                [nameof(Message)] = Message
            };
        }

        public static void AddLicenseLimitNotification(ServerStore serverStore, LicenseLimit licenseLimit)
        {
            var alert = AlertRaised.Create(
                "You've reached your license limit",
                licenseLimit.Message,
                AlertType.LicenseManager_LicenseLimit,
                NotificationSeverity.Warning,
                details: new LicenseLimitWarning(licenseLimit));

            serverStore.NotificationCenter.Add(alert);
        }
    }
}
