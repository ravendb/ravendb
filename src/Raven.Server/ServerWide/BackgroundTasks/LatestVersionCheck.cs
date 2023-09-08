using System;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Logging;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.BackgroundTasks
{
    public class LatestVersionCheck
    {
        private readonly ServerStore _serverStore;
        private const string ApiRavenDbNet = "https://api.ravendb.net";

        private static readonly Logger Logger = LoggingSource.Instance.GetLogger("Server", typeof(LatestVersionCheck).FullName);

        private readonly SemaphoreSlim _locker = new SemaphoreSlim(1, 1);

        private VersionInfo _lastRetrievedVersionInfo;

        private Timer _timer;

        private string _releaseChannel;

        private static readonly RavenHttpClient ApiRavenDbClient = new()
        {
            BaseAddress = new Uri(ApiRavenDbNet)
        };

        public LatestVersionCheck(ServerStore serverStore)
        {
            _serverStore = serverStore;
        }

        public void Initialize()
        {
            if (_serverStore.Configuration.Updates.BackgroundChecksDisabled)
                return;

            _releaseChannel = _serverStore.Configuration.Updates.Channel.ToString();
            _timer = new Timer(async state => await PerformAsync(), null, (int)TimeSpan.FromMinutes(5).TotalMilliseconds, (int)TimeSpan.FromHours(12).TotalMilliseconds);
        }

        public VersionInfo GetLastRetrievedVersionUpdatesInfo()
        {
            return _lastRetrievedVersionInfo;
        }

        public async Task PerformAsync()
        {
            await _locker.WaitAsync();

            try
            {
                var buildNumber = ServerVersion.Build;
                if (buildNumber == ServerVersion.DevBuildNumber)
                    return;

                var url = $"/api/v2/versions/latest?channel={_releaseChannel}&build={buildNumber}";
                var licenseStatus = _serverStore.LicenseManager.LicenseStatus;
                var licenseId = licenseStatus.Id;
                if (licenseId != null)
                    url += $"&licenseId={licenseId}";

                var stream = await ApiRavenDbClient.GetStreamAsync(url);

                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var json = await context.ReadForMemoryAsync(stream, "latest/version");
                    var latestVersionInfo = JsonDeserializationServer.LatestVersionCheckVersionInfo(json);

                    if (latestVersionInfo?.BuildNumber > buildNumber)
                    {
                        var severityInfo = DetermineSeverity(latestVersionInfo);

                        var alert = AlertRaised.Create(null, "RavenDB update available", $"Version {latestVersionInfo.Version} is available",
                            AlertType.Server_NewVersionAvailable, severityInfo,
                            details: new NewVersionAvailableDetails(latestVersionInfo, licenseStatus));

                        AddAlertToNotificationCenter(alert);
                    }

                    _lastRetrievedVersionInfo = latestVersionInfo;
                }
            }
            catch (Exception err)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Error getting latest version info.", err);
            }
            finally
            {
                _locker.Release();
            }
        }

        private void AddAlertToNotificationCenter(AlertRaised alert)
        {
            try
            {
                _serverStore.NotificationCenter.Add(alert);
            }
            catch (Exception err)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Error adding latest version alert to notification center.", err);
            }
        }

        private static NotificationSeverity DetermineSeverity(VersionInfo latestVersionInfo)
        {
            return Enum.Parse<NotificationSeverity>(latestVersionInfo.UpdateSeverity);
        }

        public class VersionInfo
        {
            public string Version { get; set; }

            public int BuildNumber { get; set; }

            public string BuildType { get; set; }

            public DateTime PublishedAt { get; set; }

            public string UpdateSeverity { get; set; }

            public LatestVersion LatestVersion { get; set; }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue(GetType())
                {
                    [nameof(Version)] = Version,
                    [nameof(BuildNumber)] = BuildNumber,
                    [nameof(BuildType)] = BuildType,
                    [nameof(PublishedAt)] = PublishedAt,
                    [nameof(UpdateSeverity)] = UpdateSeverity,
                    [nameof(LatestVersion)] = LatestVersion?.ToJson()
                };
            }
        }

        public class LatestVersion
        {
            public string Version { get; set; }

            public string CanUpgrade { get; set; }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue(GetType())
                {
                    [nameof(Version)] = Version,
                    [nameof(CanUpgrade)] = CanUpgrade
                };
            }
        }
    }
}
