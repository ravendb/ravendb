using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Logging;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.BackgroundTasks
{
    public static class LatestVersionCheck
    {
        private const string ApiRavenDbNet = "https://api.ravendb.net";

        private static SemaphoreSlim _locker = new SemaphoreSlim(1, 1);

        private static VersionInfo _lastRetrievedVersionInfo = null;

        private static readonly Logger _logger = LoggingSource.Instance.GetLogger("Server", typeof(LatestVersionCheck).FullName);

        private static AlertRaised _alert;

        // ReSharper disable once NotAccessedField.Local
        private static Timer _timer;

        private static readonly ConcurrentSet<WeakReference<ServerStore>> ServerStores = new ConcurrentSet<WeakReference<ServerStore>>();

        private static readonly HttpClient ApiRavenDbClient = new HttpClient
        {
            BaseAddress = new Uri(ApiRavenDbNet)
        };

        static LatestVersionCheck()
        {
            _timer = new Timer(async state => await PerformAsync(), null, (int)TimeSpan.FromMinutes(5).TotalMilliseconds, (int)TimeSpan.FromHours(12).TotalMilliseconds);
        }

        public static void Check(ServerStore serverStore)
        {
            ServerStores.Add(new WeakReference<ServerStore>(serverStore));

            var alert = _alert;
            if (alert == null)
                return;

            serverStore.NotificationCenter.Add(_alert);
        }

        public static VersionInfo GetLastRetrievedVersionUpdatesInfo()
        {
            return _lastRetrievedVersionInfo;
        }

        public static async Task PerformAsync()
        {
            await _locker.WaitAsync();

            try
            {
                var buildNumber = ServerVersion.Build;
                if (buildNumber == ServerVersion.DevBuildNumber)
                    return;

                var stream = await ApiRavenDbClient.GetStreamAsync(
                    $"/api/v2/versions/latest?channel=patch&build={buildNumber}");

                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var json = context.ReadForMemory(stream, "latest/version");
                    var latestVersionInfo = JsonDeserializationServer.LatestVersionCheckVersionInfo(json);

                    if (latestVersionInfo?.BuildNumber > buildNumber)
                    {
                        var severityInfo = DetermineSeverity(latestVersionInfo);

                        _alert = AlertRaised.Create(null, "RavenDB update available", $"Version {latestVersionInfo.Version} is available",
                            AlertType.Server_NewVersionAvailable, severityInfo,
                            details: new NewVersionAvailableDetails(latestVersionInfo));

                        AddAlertToNotificationCenter();
                    }

                    _lastRetrievedVersionInfo = latestVersionInfo;
                }
            }
            catch (Exception err)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Error getting latest version info.", err);
            }
            finally
            {
                _locker.Release();
            }
        }

        private static void AddAlertToNotificationCenter()
        {
            foreach (var weak in ServerStores)
            {

                if (weak.TryGetTarget(out ServerStore serverStore) == false || serverStore == null || serverStore.Disposed)
                {
                    ServerStores.TryRemove(weak);
                    continue;
                }

                try
                {
                    serverStore.NotificationCenter.Add(_alert);
                }
                catch (Exception err)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info("Error adding latest version alert to notification center.", err);
                }
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

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue(GetType())
                {
                    [nameof(Version)] = Version,
                    [nameof(BuildNumber)] = BuildNumber,
                    [nameof(BuildType)] = BuildType,
                    [nameof(PublishedAt)] = PublishedAt,
                    [nameof(UpdateSeverity)] = UpdateSeverity
                };
            }
        }
    }
}
