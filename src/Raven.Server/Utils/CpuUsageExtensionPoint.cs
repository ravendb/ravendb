using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using Raven.Client.Extensions;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Timer = System.Timers.Timer;

namespace Raven.Server.Utils
{
    public class CpuUsageExtensionPoint : IDisposable
    {
        private readonly JsonContextPool _contextPool;
        private readonly Logger _logger;
        private readonly NotificationCenter.NotificationCenter _notificationCenter;
        private readonly ProcessStartInfo _startInfo;
        private readonly TimeSpan _timeout = TimeSpan.FromSeconds(10);

        private Process _process;
        private bool _didRestart;

        private ExtensionPointRawData _data = new ExtensionPointRawData();
        public ExtensionPointRawData BadData = new ExtensionPointRawData
        {
            ProcessCpuUsage = -1,
            MachineCpuUsage = -1
        };

        public ExtensionPointRawData Data
        {
            get
            {
                if (IsDisposed && _logger.IsOperationsEnabled)
                {
                    _logger.Operations($"Try to get data from {nameof(CpuUsageExtensionPoint)} which was disposed");
                }
                return _data;
            }
        }

        public bool IsDisposed { get; private set; }


        public CpuUsageExtensionPoint(
            JsonContextPool contextPool,
            string exec,
            string args,
            Logger logger,
            NotificationCenter.NotificationCenter notificationCenter)
        {
            _contextPool = contextPool;
            _logger = logger;
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
                errors = errors + await _process.StandardError.ReadToEndAsync();
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
            _data = BadData;
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
                    var blittable = context.ReadForMemory(data, "cpuUsageExtensionPointData");
                    
                    if (TryGetValue(blittable, nameof(ExtensionPointRawData.MachineCpuUsage), out var machineCpuUsage)
                    && TryGetValue(blittable, nameof(ExtensionPointRawData.ProcessCpuUsage), out var processCpuUsage))
                    {
                        _data.MachineCpuUsage = machineCpuUsage;
                        _data.ProcessCpuUsage = processCpuUsage;
                        return true;
                    }
                }
                catch (Exception e)
                {
                    HandleError($"Unable to parse \"{data}\" to json", e);
                }
            }

            return false;
        }

        private bool TryGetValue(BlittableJsonReaderObject blittable, string machineCpuUsageName, out double cpuUsage)
        {
            if (blittable.TryGet(machineCpuUsageName, out cpuUsage) == false)
            {
                HandleError($"Can't read {machineCpuUsageName} property from : {Environment.NewLine + blittable}.");
                return false;
            }
            if (cpuUsage < 0 || cpuUsage > 100)
            {
                HandleError($"{nameof(ExtensionPointRawData.MachineCpuUsage)} should be between 0 to 100 : {Environment.NewLine + blittable}.");
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
                if (_didRestart == false)
                {
                    _didRestart = true;
                    NotifyWarning($"{msg} {Environment.NewLine}Therefore the process will restart.", e);

                    Start();
                }
                else
                {
                    NotifyWarning($"{msg} {Environment.NewLine}Therefore the process will terminate.", e);
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

    public class ExtensionPointRawData
    {
        public double MachineCpuUsage { get; set; }

        public double ProcessCpuUsage { get; set; }
    }
}
