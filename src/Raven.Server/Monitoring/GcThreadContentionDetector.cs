using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime;
using System.Threading;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Monitoring
{
    public sealed class GcThreadContentionDetector : IDisposable
    {
        private static readonly TimeSpan InitialCheckDelay = TimeSpan.FromSeconds(30);
        private const double MinimumContentionRatioThreshold = 0.5;
        private const int MinimumCoreDifference = 8;

        private readonly ServerStore _serverStore;
        private readonly ServerNotificationCenter _notificationCenter;
        private readonly Logger _logger = LoggingSource.Instance.GetLogger<GcThreadContentionDetector>(nameof(GcThreadContentionDetector));
        private Timer _timer;

        public GcThreadContentionDetector(ServerStore serverStore, ServerNotificationCenter notificationCenter)
        {
            _serverStore = serverStore;
            _notificationCenter = notificationCenter;

            _timer = new Timer(CheckGcThreadContention, null, InitialCheckDelay, Timeout.InfiniteTimeSpan);
        }

        private void CheckGcThreadContention(object state)
        {
            try
            {
                if (GCSettings.IsServerGC == false)
                    return;

                var totalCores = ProcessorInfo.ProcessorCount;
                var utilizedCores = _serverStore.LicenseManager.GetNumberOfUtilizedCores();
                var coreDifference = totalCores - utilizedCores;
                var contentionRatio = (double)coreDifference / utilizedCores;

                if (coreDifference < MinimumCoreDifference || contentionRatio < MinimumContentionRatioThreshold)
                {
                    DismissAlert();
                    return;
                }

                IReadOnlyDictionary<string, object> gcConfig = GC.GetConfigurationVariables();

                if (gcConfig.TryGetValue("HeapCount", out object heapCount))
                {
                    var heapCountInt64 = Convert.ToInt64(heapCount);

                    if (heapCountInt64 <= utilizedCores)
                    {
                        DismissAlert();
                        return;
                    }
                }


                RaiseAlert(totalCores, utilizedCores);
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations("Failed to check if there might be GC threads contention", e);
            }
            finally
            {
                _timer?.Dispose();
                _timer = null;
            }
        }

        private void RaiseAlert(int totalCores, int utilizedCores)
        {
            var details = new GcThreadContentionDetails
            {
                TotalCores = totalCores,
                UtilizedCores = utilizedCores,
                Message = $"Your server has {totalCores} CPU cores but is limited to use only {utilizedCores} core(s) by the license. " +
                          $"With Server GC enabled, .NET creates one GC thread per core ({totalCores} threads), but all threads are forced to run on {utilizedCores} core(s). " +
                          $"This can cause severe GC pauses due to thread contention. " +
                          $"To resolve this, configure the 'System.GC.HeapCount' setting in your Raven.Server.runtimeconfig.json file or set 'DOTNET_GCHeapCount' env variable to " +
                          $"match your utilized cores ({utilizedCores})."
            };

            var alert = AlertRaised.Create(
                null,
                "Possible GC thread contention detected",
                details.Message,
                AlertType.GcThreadContention,
                NotificationSeverity.Warning,
                details: details);

            _notificationCenter.Add(alert);

            if (_logger.IsOperationsEnabled)
            {
                _logger.Operations($"GC thread contention detected: {totalCores} total cores but only {utilizedCores} utilized core(s). " +
                                  $"Consider configuring System.GC.HeapCount={utilizedCores} in Raven.Server.runtimeconfig.json or set DOTNET_GCHeapCount={utilizedCores:X} env variable");
            }
        }


        private void DismissAlert()
        {
            _notificationCenter.Dismiss(AlertRaised.GetKey(AlertType.GcThreadContention, null));
        }

        public void Dispose()
        {
            _timer?.Dispose();
            _timer = null;
        }
    }
}
