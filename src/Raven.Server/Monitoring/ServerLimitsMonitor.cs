using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client.Documents.Conventions;
using Raven.Client.Util;
using Raven.Server.Logging;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Platform.Posix;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Platform;

namespace Raven.Server.Monitoring
{
    public class ServerLimitsMonitor : IDisposable
    {
        private const string Source = "server-limits";

        private static readonly TimeSpan CheckFrequency = TimeSpan.FromMinutes(5);

        private const float MaxMapCountPercentThreshold = 0.05f;
        private const int MaxMapCountNumberThreshold = 1024 * 10;

        private const float MaxThreadsThreshold = 0.05f;
        private const int MaxThreadsNumberThreshold = 1024 * 10;

        private readonly ServerStore _serverStore;
        private readonly ServerNotificationCenter _notificationCenter;
        private readonly NotificationsStorage _notificationsStorage;
        private readonly RavenLogger _logger = RavenLogManager.Instance.GetLoggerForServer<ServerLimitsMonitor>();
        private readonly List<ServerLimitsDetails.ServerLimitInfo> _alerts = new();
        private readonly object _runLock = new();

        private Timer _timer;

        public ServerLimitsMonitor(ServerStore serverStore, ServerNotificationCenter notificationCenter, NotificationsStorage notificationsStorage)
        {
            _serverStore = serverStore;
            _notificationCenter = notificationCenter;
            _notificationsStorage = notificationsStorage;

            if (PlatformDetails.RunningOnPosix == false || PlatformDetails.RunningOnMacOsx)
            {
                return;
            }

            _timer = new Timer(Run, null, CheckFrequency, CheckFrequency);
        }

        internal void Run(object state)
        {
            if (_notificationCenter.IsInitialized == false)
                return;

            if (Monitor.TryEnter(_runLock) == false)
                return;

            try
            {
                var maxLimits = _serverStore.Server.MetricCacher.GetValue<LimitsInfo>(MetricCacher.Keys.Server.MaxServerLimits);
                var currentLimits = _serverStore.Server.MetricCacher.GetValue<LimitsInfo>(MetricCacher.Keys.Server.CurrentServerLimits);

                if (maxLimits == null || currentLimits == null)
                    return;

                AddAlertIfNeeded(currentLimits.MapCountCurrent, maxLimits.MapCountMax, MaxMapCountPercentThreshold, MaxMapCountNumberThreshold,
                    LimitsReader.MaxMapCountFilePath, "Map count");
                AddAlertIfNeeded(currentLimits.ThreadsCurrent, maxLimits.ThreadsMax, MaxThreadsThreshold, MaxThreadsNumberThreshold, LimitsReader.ThreadsMaxFilePath,
                    "Threads number");

                if (_alerts.Count > 0)
                {
                    ServerLimitsDetails details;
                    AlertRaised hint;
                    var id = AlertRaised.GetKey(AlertType.ServerLimits, Source);

                    using (_notificationsStorage.Read(id, out var ntv))
                    {
                        using (ntv)
                        {
                            if (ntv == null ||
                                ntv.Json.TryGet(nameof(PerformanceHint.Details), out BlittableJsonReaderObject detailsJson) == false ||
                                detailsJson == null)
                            {
                                details = new ServerLimitsDetails();
                            }
                            else
                            {
                                details = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<ServerLimitsDetails>(detailsJson);
                            }
                        }

                        hint = AlertRaised.Create(
                            null,
                            "Running close to OS limits",
                            "We have detected server is running close to OS limits",
                            AlertType.ServerLimits,
                            NotificationSeverity.Warning,
                            Source,
                            details
                        );
                    }

                    foreach (var limitInfo in _alerts)
                    {
                        details.Limits.AddFirst(limitInfo);
                        if (details.Limits.Count > ServerLimitsDetails.MaxNumberOfLimits)
                        {
                            details.Limits.RemoveLast();
                        }
                    }

                    _notificationCenter.Add(hint);

                    if (_logger.IsWarnEnabled)
                    {
                        _logger.Warn($"Running close to OS limits detected:{Environment.NewLine}{GetAlertsString()}");
                    }
                }
            }
            catch (Exception e)
            {
                if (_logger.IsErrorEnabled)
                {
                    var addition = string.Empty;
                    if (_alerts.Count > 0)
                    {
                        addition = $" Last Alerts:{Environment.NewLine}{GetAlertsString()}";
                    }
                    _logger.Error($"Failed to run '{nameof(ServerLimitsMonitor)}'{addition}", e);
                }
            }
            finally
            {
                _alerts.Clear();
                Monitor.Exit(_runLock);
            }
        }

        private string GetAlertsString()
        {
            return string.Join(Environment.NewLine,
                _alerts.Select(x =>
                    $"{x.Name} current value is '{x.Value}' which is close to the OS limit '{x.Max}', please increase the limit. The parameter is defined in '{x.Limit}' file."));
        }

        private void AddAlertIfNeeded(long current, long max, float percentThreshold, int numberThreshold, string limit, string name)
        {
            if (current > max - numberThreshold && current >= (long)(max * (1 - percentThreshold)))
            {
                var limitInfo = new ServerLimitsDetails.ServerLimitInfo(name, limit, current, max, SystemTime.UtcNow);
                _alerts.Add(limitInfo);
            }
        }

        public void Dispose()
        {
            _timer?.Dispose();
            _timer = null;
        }
    }
}
