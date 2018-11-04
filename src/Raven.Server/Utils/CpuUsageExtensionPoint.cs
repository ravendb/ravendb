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
            ActiveCores = -1,
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

            InitNewProcessHandle();
        }

        public void Start()
        {
            try
            {
                _process.Start();
            }
            catch (Exception e)
            {
                NotifyWarning("Could not start cpu usage extension point process", e);

                Dispose();
            }

            var _ = ReadProcess(); // explicitly starting async task without waiting for it
            _ = ReadErrors();
        }

        private async Task ReadErrors()
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
            errors = errors + await _process.StandardError.ReadLineAsync();
            NotifyWarning($"Extension point process send an error: {errors}");
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
                    var blittable = context.ReadForMemory(data, string.Empty);
                    if (blittable.TryGet(nameof(ExtensionPointRawData.MachineCpuUsage), out double machineCpuUsage) == false)
                    {
                        HandleError($"Can't read {nameof(ExtensionPointRawData.MachineCpuUsage)} property from : \n{blittable}.");
                        return false;
                    }
                    _data.MachineCpuUsage = machineCpuUsage;

                    if (blittable.TryGet(nameof(ExtensionPointRawData.ActiveCores), out double activeCores) == false)
                    {
                        HandleError($"Can't read {nameof(ExtensionPointRawData.ActiveCores)} property from : \n{blittable}.");
                        return false;
                    }
                    _data.ActiveCores = activeCores;
                }
                catch (Exception e)
                {
                    HandleError($"Unable to parse \"{data}\" to json", e);
                    return false;
                }
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
                    NotifyWarning($"{msg} \nTherefore the process will restart.", e);

                    InitNewProcessHandle();
                    Start();
                }
                else
                {
                    NotifyWarning($"{msg} \nTherefore the process will terminate.", e);
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

        private void InitNewProcessHandle()
        {
            _process = new Process
            {
                StartInfo = _startInfo,
                EnableRaisingEvents = true
            };
        }
    }

    public class ExtensionPointRawData
    {
        public double MachineCpuUsage { get; set; }

        public double ActiveCores { get; set; }
    }
}
