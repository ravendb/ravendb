using Raven.Client.Exceptions.Commercial;
using Raven.Server.Web.System;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    internal class LicenseLimitWarning : INotificationDetails
    {
        private LicenseLimitWarning()
        {
            
        }

        private LicenseLimitWarning(LicenseLimitException licenseLimit)
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

        public static void AddLicenseLimitNotification(ServerNotificationCenter notificationCenter, LicenseLimitException licenseLimit)
        {
            var alert = AlertRaised.Create(
                null,
                $@"You've reached your license limit ({EnumHelper.GetDescription(licenseLimit.Type)})",
                licenseLimit.Message,
                AlertType.LicenseManager_LicenseLimit,
                NotificationSeverity.Warning,
                key: licenseLimit.Type.ToString(),
                details: new LicenseLimitWarning(licenseLimit));

            notificationCenter.Add(alert, updateExisting: true);
        }

        public static void DismissLicenseLimitNotification(ServerNotificationCenter notificationCenter, LimitType type)
        {
            notificationCenter.Dismiss(AlertRaised.GetKey(AlertType.LicenseManager_LicenseLimit, type.ToString()));
        }
    }
}
