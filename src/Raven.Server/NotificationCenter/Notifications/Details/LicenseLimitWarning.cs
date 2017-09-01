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
            Details = licenseLimit.Details;
        }

        public LimitType Type { get; set; }

        public string Details { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(LimitType)] = Type,
                [nameof(Details)] = Details
            };
        }

        public static void AddLicenseLimitNotification(ServerStore serverStore, LicenseLimit licenseLimit)
        {
            var alert = AlertRaised.Create(
                "You've reached your licnese limit",
                licenseLimit.Details,
                AlertType.LicenseManager_LicenseLimit,
                NotificationSeverity.Warning,
                details: new LicenseLimitWarning(licenseLimit));

            serverStore.NotificationCenter.Add(alert);
        }
    }
}
