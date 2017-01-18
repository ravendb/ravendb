using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Sparrow.Logging;
using Raven.Abstractions.Util;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Actions.Details;
using Raven.Server.NotificationCenter.Actions.Server;
using Raven.Server.NotificationCenter.Alerts;
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
                var apiRavenDbClient = new HttpClient()
                {
                    BaseAddress = new Uri(ApiRavenDbNet)
                };

                // TODO @gregolsky make channel customizable 
                var stream =
                    await apiRavenDbClient.GetStreamAsync("/api/v1/versions/latest?channel=dev&min=40000&max=49999");

                JsonOperationContext context;
                using (_serverStore.ContextPool.AllocateOperationContext(out context))
                {
                    var json = context.ReadForMemory(stream, "latest/version");
                    var latestVersionInfo = JsonDeserializationServer.LatestVersionCheckVersionInfo(json);

                    if (ServerVersion.Build != ServerVersion.DevBuildNumber && 
                        latestVersionInfo?.BuildNumber > ServerVersion.Build)
                    {
                        var severityInfo = DetermineSeverity(latestVersionInfo);
                        
                        var alert = RaiseServerAlert.Create("RavenDB update available", $"Version {latestVersionInfo.Version} is avaiable",
                            ServerAlertType.NewServerVersionAvailable, severityInfo,
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

        private static AlertSeverity DetermineSeverity(VersionInfo latestVersionInfo)
        {
            var diff = SystemTime.UtcNow - latestVersionInfo.PublishedAt;
            var severityInfo = AlertSeverity.Info;
            if (diff.TotalDays > 21)
            {
                severityInfo = AlertSeverity.Error;
            }
            else if (diff.TotalDays > 7)
            {
                severityInfo = AlertSeverity.Warning;
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
        }
    }
}
