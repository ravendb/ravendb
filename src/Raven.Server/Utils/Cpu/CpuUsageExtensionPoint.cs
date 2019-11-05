using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Raven.Server.Dashboard;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server.Platform;

namespace Raven.Server.Utils.Cpu
{
    public class CpuUsageExtensionPoint
    {
        private readonly JsonContextPool _contextPool;
        private readonly Logger _logger = LoggingSource.Instance.GetLogger<MachineResources>("Server");
        private readonly NotificationCenter.NotificationCenter _notificationCenter;
        private readonly ProcessStartInfo _startInfo;

        private ExtensionPointData _data;

        private static readonly ExtensionPointData BadData = new ExtensionPointData
        {
            ProcessCpuUsage = -1,
            MachineCpuUsage = -1
        };

        private long _isDataValid = 0;
        public ExtensionPointData Data => Interlocked.Read(ref _isDataValid) == 0 ? BadData : _data;


        public CpuUsageExtensionPoint(
            JsonContextPool contextPool,
            string exec,
            string args,
            NotificationCenter.NotificationCenter notificationCenter)
        {
            _contextPool = contextPool;
            _notificationCenter = notificationCenter;
            _startInfo = new ProcessStartInfo
            {
                FileName = exec,
                Arguments = args
            };
        }

        public void Start(CancellationToken serverShutdown)
        {
            Task.Run(() =>
            {
                this.StartInternal(serverShutdown);
            }, serverShutdown);
        }


        private void StartInternal(CancellationToken ctk)
        {
            try
            {
                var retry = 2;
                const int maxMinutesBetweenIssues = 15;
                var lastRestart = DateTime.UtcNow;
                var lastReceivedLine = DateTime.UtcNow;
                while (retry-- > 0)
                {
                    using (var cts = new CancellationTokenSource())
                    using (var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, ctk))
                    {
                        var lineOutHandler = new Action<object, string>((p, l) =>
                        {
                            Interlocked.Exchange(ref _isDataValid, 1);

                            if (l == null)
                            {
                                if (DateTime.UtcNow - lastReceivedLine > TimeSpan.FromSeconds(60))
                                {
                                    Interlocked.Exchange(ref _isDataValid, 0);
                                    NotifyWarning("Cpu usage process hanged (no output for 60 seconds), killing the process",
                                        new TimeoutException("no output from process for 60 seconds"));
                                    cts.Cancel();
                                }

                                return;
                            }

                            lastReceivedLine = DateTime.UtcNow;
                            var errString = TryHandleInfoReceived(l);
                            if (errString != null)
                            {
                                Interlocked.Exchange(ref _isDataValid, 0);
                                NotifyWarning(errString);
                                cts.Cancel();
                            }
                        });

                        RavenProcess.Execute(
                            _startInfo.FileName,
                            _startInfo.Arguments,
                            60,
                            null,
                            lineOutHandler,
                            linkedCts.Token);

                        Interlocked.Exchange(ref _isDataValid, 0);

                        if (ctk.IsCancellationRequested)
                            return;

                        if (DateTime.UtcNow - lastRestart > TimeSpan.FromMinutes(maxMinutesBetweenIssues))
                        {
                            retry = 2;
                            lastRestart = DateTime.UtcNow;
                            NotifyWarning($"'Restarting '{_startInfo.FileName} {_startInfo.Arguments}'.", new TimeoutException("Cpu usage process restart"));
                        }
                    }
                }

                NotifyWarning(
                        $"Failed twice in {maxMinutesBetweenIssues} minutes to get cpu usage vi extension point process withing and therefore the process will terminate.",
                        new TimeoutException("Cpu usage process terminate"));
            }
            catch (Exception e)
            {
                NotifyWarning("Could not start cpu usage extension point process", e);
                // ignore
            }
        }

        private string TryHandleInfoReceived(string data)
        {
            if (data == null)
            {
                return "The output stream of the process has closed.";
            }
            using (_contextPool.AllocateOperationContext(out var context))
            {
                try
                {
                    using (var blittable = context.ReadForMemory(data, "cpuUsageExtensionPointData"))
                    {
                        if (TryGetCpuUsage(blittable, nameof(ExtensionPointData.MachineCpuUsage), out var machineCpuUsage)
                            && TryGetCpuUsage(blittable, nameof(ExtensionPointData.ProcessCpuUsage), out var processCpuUsage))
                        {
                            _data.MachineCpuUsage = machineCpuUsage;
                            _data.ProcessCpuUsage = processCpuUsage;
                            return null; // success
                        }
                        else
                        {
                            return $"Failed to TryGetCpuUsage for MachineCpuUsage or ProcessCpuUsage items from cpuUsageExtensionPointData. Data:'{data}'";
                        }
                    }
                }
                catch (Exception e)
                {
                    return $"Unable to parse \"{data}\" to json. Exception:" + e;
                }
            }
        }

        private bool TryGetCpuUsage(BlittableJsonReaderObject blittable, string propertyName, out double cpuUsage)
        {
            if (blittable.TryGet(propertyName, out cpuUsage) == false)
                return false;
            if (cpuUsage < 0)
                return false;

            if (cpuUsage > 100)
                cpuUsage = 100;

            return true;
        }

        private void NotifyWarning(string warningMsg, Exception e = null)
        {
            if (_logger.IsOperationsEnabled)
            {
                _logger.Operations(warningMsg, e);
            }

            try
            {
                var alert = AlertRaised.Create(
                    null,
                    "Cpu usage extension point error",
                    warningMsg,
                    AlertType.CpuUsageExtensionPointError,
                    NotificationSeverity.Warning);
                _notificationCenter.Add(alert);
            }
            catch
            {
                // nothing to do if we can't report it
            }
        }
    }

    public struct ExtensionPointData
    {
        public double MachineCpuUsage { get; set; }

        public double ProcessCpuUsage { get; set; }
    }
}
