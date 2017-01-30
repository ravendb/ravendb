using Raven.Server.ServerWide.BackgroundTasks;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Actions.Details
{
    public class NewVersionAvailableDetails : IActionDetails
    {
        public NewVersionAvailableDetails(LatestVersionCheck.VersionInfo versionInfo)
        {
            VersionInfo = versionInfo;
        }

        public LatestVersionCheck.VersionInfo VersionInfo { get; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(VersionInfo.Version)] = VersionInfo.Version,
                [nameof(VersionInfo.BuildNumber)] = VersionInfo.BuildNumber,
                [nameof(VersionInfo.BuildType)] = VersionInfo.BuildType,
                [nameof(VersionInfo.PublishedAt)] = VersionInfo.PublishedAt
            };
        }
    }
}