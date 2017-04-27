using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;
using Sparrow.Logging;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.BackgroundTasks
{
    public class LatestVersionCheck : IDisposable
    {
        private const string ApiRavenDbNet = "https://api.ravendb.net";

        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<LatestVersionCheck>(null);

        private Timer _latestVersionCheckTimer;

        private readonly ServerStore _serverStore;

        private static readonly HttpClient ApiRavenDbClient = new HttpClient()
        {
            BaseAddress = new Uri(ApiRavenDbNet)
        };

        public LatestVersionCheck(ServerStore serverStore)
        {
            _serverStore = serverStore;
        }

        public void Initialize()
        {
            _latestVersionCheckTimer = new Timer((state) =>
                AsyncHelpers.RunSync(PerformAsync), null, 0, (int)TimeSpan.FromHours(12).TotalMilliseconds);
        }

        private async Task PerformAsync()
        {
            try
            {
                // TODO @gregolsky make channel customizable 
                var stream =
                    await ApiRavenDbClient.GetStreamAsync("/api/v1/versions/latest?channel=dev&min=40000&max=49999");

                JsonOperationContext context;
                using (_serverStore.ContextPool.AllocateOperationContext(out context))
                {
                    var json = context.ReadForMemory(stream, "latest/version");
                    var latestVersionInfo = JsonDeserializationServer.LatestVersionCheckVersionInfo(json);

                    if (ServerVersion.Build != ServerVersion.DevBuildNumber && 
                        latestVersionInfo?.BuildNumber > ServerVersion.Build)
                    {
                        var severityInfo = DetermineSeverity(latestVersionInfo);
                        
                        var alert = AlertRaised.Create("RavenDB update available", $"Version {latestVersionInfo.Version} is avaiable",
                            AlertType.Server_NewVersionAvailable, severityInfo,
                            details: new NewVersionAvailableDetails(latestVersionInfo));

                        _serverStore.NotificationCenter.Add(alert);
                    }
                }
            }
            catch (Exception err)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Error getting latest version info.", err);
            }
        }

        private static NotificationSeverity DetermineSeverity(VersionInfo latestVersionInfo)
        {
            var diff = SystemTime.UtcNow - latestVersionInfo.PublishedAt;
            var severityInfo = NotificationSeverity.Info;
            if (diff.TotalDays > 21)
            {
                severityInfo = NotificationSeverity.Error;
            }
            else if (diff.TotalDays > 7)
            {
                severityInfo = NotificationSeverity.Warning;
            }
            return severityInfo;
        }

        public void Dispose()
        {
            _latestVersionCheckTimer?.Dispose();
        }

        public class VersionInfo
        {
            public string Version { get; set; }

            public int BuildNumber { get; set; }

            public string BuildType { get; set; }

            public DateTime PublishedAt { get; set; }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue(GetType())
                {
                    [nameof(Version)] = Version,
                    [nameof(BuildNumber)] = BuildNumber,
                    [nameof(BuildType)] = BuildType,
                    [nameof(PublishedAt)] = PublishedAt
                };
            }
        }
    }
}
