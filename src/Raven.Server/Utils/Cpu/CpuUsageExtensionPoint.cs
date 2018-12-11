using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Raven.Server.Dashboard;
using Raven.Server.NotificationCenter.Notifications;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Utils.Cpu
{
    public class CpuUsageExtensionPoint : IDisposable
    {
        private readonly JsonContextPool _contextPool;
        private readonly Logger _logger = LoggingSource.Instance.GetLogger<MachineResources>("Server");
        private readonly NotificationCenter.NotificationCenter _notificationCenter;
        private readonly ProcessStartInfo _startInfo;
        private readonly TimeSpan _timeout = TimeSpan.FromSeconds(10);

        private Process _process;
        private DateTime _lastRestart;

        private ExtensionPointData _data;
        public ExtensionPointData BadData = new ExtensionPointData
        {
            ProcessCpuUsage = -1,
            MachineCpuUsage = -1
        };

        public ExtensionPointData Data => IsDisposed ? BadData : _data;

        public bool IsDisposed { get; private set; }


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
                Arguments = args,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true
            };
        }

        public void Start()
        {
            _process = new Process
            {
                StartInfo = _startInfo,
                EnableRaisingEvents = true
            };

            try
            {
                _process.Start();
            }
            catch (Exception e)
            {
                NotifyWarning("Could not start cpu usage extension point process", e);

                Dispose();

                return;
            }

            var _ = ReadProcess(); // explicitly starting async task without waiting for it
            _ = ReadErrors();
        }

        private async Task ReadErrors()
        {
            try
            {
                var errors = await _process.StandardError.ReadLineAsync();
                if (errors == null)
                {
                    return;
                }
                try
                {
                    if (_process.HasExited == false)
                    {
                        _process.Kill();
                    }
                }
                catch (Exception)
                {
                    // When the process terminating, killed or exited
                }
                errors = errors + Environment.NewLine + await _process.StandardError.ReadToEndAsync();
                NotifyWarning($"Extension point process send an error: {errors}");
            }
            catch (Exception e)
            {
                NotifyWarning("Could not read errors from cpu usage extension point process", e);
            }
        }

        public void Dispose()
        {
            IsDisposed = true;
            using (_process)
            {
                try
                {
                    if (_process.HasExited == false)
                    {
                        _process.Kill();
                    }
                }
                catch (Exception)
                {
                    // When the process terminating, killed or exited
                }

                _process = null;
            }
        }


        private async Task ReadProcess()
        {
            try
            {
                while (IsDisposed == false)
                {
                    var nextLine = _process.StandardOutput.ReadLineAsync();

                    if (await nextLine.WaitWithTimeout(_timeout) == false)
                    {
                        // here we missed an update, restart the process once
                        HandleError($"The process didn't send information for {_timeout.TotalSeconds} seconds.");
                        break;
                    }

                    var line = await nextLine;
                    if (HandleInfoReceived(line) == false)
                    {
                        break;
                    }
                }
            }
            catch (Exception e)
            {
                HandleError(e.Message, e);
            }
        }

        private bool HandleInfoReceived(string data)
        {
            if (data == null)
            {
                HandleError("The output stream of the process has closed.");
                return false;
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
                            return true;
                        }
                        // TryGetCpuUsage call HandleError when it failed
                    }
                }
                catch (Exception e)
                {
                    HandleError($"Unable to parse \"{data}\" to json", e);
                }
            }

            return false;
        }

        private bool TryGetCpuUsage(BlittableJsonReaderObject blittable, string propertyName, out double cpuUsage)
        {
            if (blittable.TryGet(propertyName, out cpuUsage) == false)
            {
                HandleError($"Can't read {propertyName} property from : {Environment.NewLine + blittable}.");
                return false;
            }
            if (cpuUsage < 0)
            {
                HandleError($"{nameof(ExtensionPointData.MachineCpuUsage)} can't be negative : {Environment.NewLine + blittable}.");
                return false;
            }

            return true;
        }

        private void HandleError(string msg, Exception e = null)
        {
            try
            {
                if (_process.HasExited == false)
                {
                    _process.Kill();
                }
            }
            catch (Exception)
            {
                // When the process terminating, killed or exited
            }
            finally
            {
                const int maxMinutesBetweenIssues = 15;
                if (DateTime.UtcNow - _lastRestart > TimeSpan.FromMinutes(maxMinutesBetweenIssues))
                {
                    _lastRestart = DateTime.Now;
                    NotifyWarning($"{msg} {Environment.NewLine}Therefore the process will restart.", e);

                    Start();
                }
                else
                {
                    NotifyWarning($"{msg} {Environment.NewLine}This is the second issue in cpu usage extension point process withing {maxMinutesBetweenIssues} minutes and therefore the process will terminate.", e);
                    Dispose();
                }
            }
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
