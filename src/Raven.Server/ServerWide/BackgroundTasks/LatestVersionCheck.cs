using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Sparrow.Logging;
using Raven.Abstractions.Util;
using Raven.Client.Json;
using Raven.Server.Alerts;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Json;
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

                // TODO change URL
                var stream =
                    await apiRavenDbClient.GetStreamAsync("/api/versions/latest?channel=dev&min=40000&max=49999");

                JsonOperationContext context;
                using (_serverStore.ContextPool.AllocateOperationContext(out context))
                {
                    var json = context.ReadForMemory(stream, "latest/version");
                    var latestVersionInfo = JsonDeserializationServer.LatestVersionCheckVersionInfo(json);

                    if (latestVersionInfo?.BuildNumber > ServerVersion.Build)
                    {
                        var severityInfo = DetermineSeverity(latestVersionInfo);

                        _serverStore.Alerts.AddAlert(new Alert
                        {
                            Type = AlertType.NewServerVersionAvailable,
                            Severity = severityInfo,
                            Key = nameof(AlertType.NewServerVersionAvailable),
                            Content = new NewVersionAvailableAlertContent
                            {
                                VersionInfo = latestVersionInfo
                            },
                            Message = FormatMessage(latestVersionInfo),
                        });
                    }
                }
            }
            catch (Exception err)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Error getting latest version info.", err);
            }
        }

        private static string FormatMessage(VersionInfo latestVersionInfo)
        {
            return $@"
            <h3>New version!</h3>
            <p>{latestVersionInfo.Version} is available now.</p>";
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
            _latestVersionCheckTimer.Dispose();
        }

        public class VersionInfo
        {
            public string Version { get; set; }

            public int BuildNumber { get; set; }

            public string BuildType { get; set; }

            public DateTime PublishedAt { get; set; }
        }

        public class NewVersionAvailableAlertContent : IAlertContent
        {
            public VersionInfo VersionInfo { get; set; }
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
}
