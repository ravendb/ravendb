using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Config.Categories;
using Sparrow.Logging;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.BackgroundTasks
{
    public class LatestVersionCheck
    {
        private const string ApiRavenDbNet = "https://api.ravendb.net";

        private static readonly Logger Logger = LoggingSource.Instance.GetLogger("Server", typeof(LatestVersionCheck).FullName);

        public static LatestVersionCheck Instance = new LatestVersionCheck();

        private readonly SemaphoreSlim _locker = new SemaphoreSlim(1, 1);

        private VersionInfo _lastRetrievedVersionInfo;

        private AlertRaised _alert;

        private Timer _timer;

        private string _releaseChannel;

        private readonly ConcurrentSet<WeakReference<ServerStore>> _serverStores = new ConcurrentSet<WeakReference<ServerStore>>();

        private static readonly HttpClient ApiRavenDbClient = new HttpClient
        {
            BaseAddress = new Uri(ApiRavenDbNet)
        };

        private LatestVersionCheck()
        {
        }

        public void Initialize(ServerConfiguration configuration)
        {
            if (_timer != null)
                return;

            lock (_locker)
            {
                if (_timer != null)
                    return;

                _releaseChannel = configuration.ReleaseChannel.ToString();
                _timer = new Timer(async state => await PerformAsync(), null, (int)TimeSpan.FromMinutes(5).TotalMilliseconds, (int)TimeSpan.FromHours(12).TotalMilliseconds);
            }
        }

        public void Check(ServerStore serverStore)
        {
            _serverStores.Add(new WeakReference<ServerStore>(serverStore));

            var alert = _alert;
            if (alert == null)
                return;

            serverStore.NotificationCenter.Add(_alert);
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

                var stream = await ApiRavenDbClient.GetStreamAsync(
                    $"/api/v2/versions/latest?channel={_releaseChannel}&build={buildNumber}");

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
                if (Logger.IsInfoEnabled)
                    Logger.Info("Error getting latest version info.", err);
            }
            finally
            {
                _locker.Release();
            }
        }

        private void AddAlertToNotificationCenter()
        {
            foreach (var weak in _serverStores)
            {

                if (weak.TryGetTarget(out ServerStore serverStore) == false || serverStore == null || serverStore.Disposed)
                {
                    _serverStores.TryRemove(weak);
                    continue;
                }

                try
                {
                    serverStore.NotificationCenter.Add(_alert);
                }
                catch (Exception err)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info("Error adding latest version alert to notification center.", err);
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
