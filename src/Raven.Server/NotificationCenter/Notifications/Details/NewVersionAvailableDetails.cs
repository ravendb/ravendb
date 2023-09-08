using Raven.Server.ServerWide.BackgroundTasks;
using Sparrow.Json.Parsing;
using LicenseStatus = Raven.Server.Commercial.LicenseStatus;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public class NewVersionAvailableDetails : INotificationDetails
    {
        public NewVersionAvailableDetails(LatestVersionCheck.VersionInfo versionInfo, LicenseStatus licenseStatus)
        {
            VersionInfo = versionInfo;
            LicenseStatus = licenseStatus;
        }

        public LatestVersionCheck.VersionInfo VersionInfo { get; }

        public LicenseStatus LicenseStatus { get; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(VersionInfo)] = VersionInfo.ToJson(),
                [nameof(LicenseStatus.Version)] = LicenseStatus.Version
            };
        }
    }
}
