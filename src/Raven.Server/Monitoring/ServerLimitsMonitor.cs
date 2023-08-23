using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client.Documents.Conventions;
using Raven.Client.Util;
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
        private static readonly string _source = "server-limits";

        private static readonly TimeSpan CheckFrequency = TimeSpan.FromMinutes(5);

        private static readonly float MaxMapCountPercentThreshold = 0.05f;
        private static readonly int MaxMapCountNumberThreshold = 1024 * 10;

        private static readonly float MaxThreadsThreshold = 0.05f;
        private static readonly int MaxThreadsNumberThreshold = 1024 * 10;

        private readonly ServerStore _serverStore;
        private readonly NotificationCenter.NotificationCenter _notificationCenter;
        private readonly NotificationsStorage _notificationsStorage;
        private readonly Logger _logger = LoggingSource.Instance.GetLogger<ServerLimitsMonitor>(nameof(ServerLimitsMonitor));
        private readonly List<ServerLimitsDetails.ServerLimitInfo> _alerts = new List<ServerLimitsDetails.ServerLimitInfo>();
        private readonly object _runLock = new();

        private Timer _timer;

        public ServerLimitsMonitor(ServerStore serverStore, NotificationCenter.NotificationCenter notificationCenter, NotificationsStorage notificationsStorage)
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
                    LimitsReader.MaxMapCountFilePath, "Current map count");
                AddAlertIfNeeded(currentLimits.ThreadsCurrent, maxLimits.ThreadsMax, MaxThreadsThreshold, MaxThreadsNumberThreshold, LimitsReader.ThreadsMaxFilePath,
                    "Current threads number");

                if (_alerts.Count > 0)
                {
                    ServerLimitsDetails details;
                    PerformanceHint hint;
                    var id = PerformanceHint.GetKey(PerformanceHintType.ServerLimits, _source);

                    using (_notificationsStorage.Read(id, out var ntv))
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

                        hint = PerformanceHint.Create(
                            null,
                            "Running close to OS limits",
                            "We have detected server is running close to OS limits",
                            PerformanceHintType.ServerLimits,
                            NotificationSeverity.Info,
                            _source,
                            details
                        );
                    }

                    for (int i = _alerts.Count - 1; i >= 0; i--)
                    {
                        details.Limits.Add(_alerts[i]);
                    }

                    if (details.Limits.Count > ServerLimitsDetails.MaxNumberOfLimits)
                    {
                        details.Limits = details.Limits.Take(ServerLimitsDetails.MaxNumberOfLimits).ToList();
                    }

                    _notificationCenter.Add(hint);

                    if (_logger.IsOperationsEnabled)
                    {
                        _logger.Operations($"Running close to OS limits detected:{Environment.NewLine}" + string.Join(Environment.NewLine,
                            _alerts.Select(x =>
                                $"{x.Name} is '{x.Current}' which is close to the OS limit '{x.Max}', please increase the limit. The parameter is defined in '{x.Limit}' file.")));
                    }
                }
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                {
                    var addition = string.Empty;
                    if (_alerts.Count > 0)
                    {
                        addition = $"Last Alerts: {Environment.NewLine}{string.Join(Environment.NewLine,
                            _alerts.Select(x =>
                                $"{x.Name} is '{x.Current}' which is close to the OS limit '{x.Max}', please increase the limit. The parameter is defined in '{x.Limit}' file."))}";
                    }
                    _logger.Operations($"Failed to run {nameof(ServerLimitsMonitor)}" + addition, e);
                }
            }
            finally
            {
                _alerts.Clear();
                Monitor.Exit(_runLock);
            }
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
