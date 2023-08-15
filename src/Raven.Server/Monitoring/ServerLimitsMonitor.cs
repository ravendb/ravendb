using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.Storage;
using Sparrow.Logging;
using Sparrow.Platform;

namespace Raven.Server.Monitoring
{
    public class ServerLimitsMonitor : IDisposable
    {
        private static readonly TimeSpan CheckFrequency = TimeSpan.FromMinutes(5);
        private static readonly float MaxMapCountThreshold = 0.05f;
        private static readonly float MaxThreadsMaxThreshold = 0.05f;
        private static readonly float MaxPidMaxThreshold = 0.05f;
        private static readonly string pid_max = "/proc/sys/kernel/pid_max";
        private static readonly string max_map_count = "/proc/sys/vm/max_map_count";
        private static readonly string threads_max = "/proc/sys/kernel/threads-max";

        private readonly Logger _logger = LoggingSource.Instance.GetLogger<StorageSpaceMonitor>(nameof(ServerLimitsMonitor));
        private readonly object _runLock = new();
        private readonly NotificationCenter.NotificationCenter _notificationCenter;
        private readonly List<string> _alerts = new List<string>();
        private readonly Dictionary<string, long> _maxLimitsDictionary = new Dictionary<string, long>();

        private Timer _timer;

        public ServerLimitsMonitor(NotificationCenter.NotificationCenter notificationCenter)
        {
            _notificationCenter = notificationCenter;

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
                CheckMaxMapCountLimits();
                CheckThreadsMaxLimits();
                CheckPidMaxLimits();

                if (_alerts.Count > 0)
                {
                    var warningMsg = string.Join(Environment.NewLine, _alerts);
                    if (_logger.IsOperationsEnabled)
                    {
                        _logger.Operations($"Running close to OS limits detected:{Environment.NewLine}" + warningMsg);
                    }

                    var alert = AlertRaised.Create(
                        null,
                        "Running close to OS limits",
                        warningMsg,
                        AlertType.LowServerLimits,
                        NotificationSeverity.Warning);

                    _notificationCenter.Add(alert);
                }
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Failed to run {nameof(ServerLimitsMonitor)}", e);
            }
            finally
            {
                _alerts.Clear();
                Monitor.Exit(_runLock);
            }
        }

        private void CheckPidMaxLimits()
        {
            var maxPidMax = GetMaxValueForLimit(pid_max);

            using var proc = new Process();
            proc.StartInfo.RedirectStandardOutput = true;
            proc.StartInfo.RedirectStandardError = true;
            proc.StartInfo.FileName = "ps";
            proc.StartInfo.Arguments = @"-efL";
            proc.Start();

            proc.WaitForExit();

            if (proc.ExitCode != 0)
            {
                throw new InvalidOperationException($"Executing '{proc.StartInfo.FileName} {proc.StartInfo.Arguments}' failed (exit code {proc.ExitCode}){Environment.NewLine}StandardOutput:{Environment.NewLine}{proc.StandardOutput.ReadToEnd()}{Environment.NewLine}StandardError:{Environment.NewLine}{proc.StandardError.ReadToEnd()}");
            }

            var currentPidMax = GetEolCount(proc.StandardOutput.BaseStream);

            AddAlertIfNeeded(currentPidMax, maxPidMax, MaxPidMaxThreshold, pid_max, nameof(pid_max));
        }

        private void CheckThreadsMaxLimits()
        {
            var maxThreadsMax = GetMaxValueForLimit(threads_max);

            var currentThreadsMax = 0L;
            foreach (var _ in Directory.EnumerateDirectories("/proc/self/task", "*", SearchOption.TopDirectoryOnly))
            {
                currentThreadsMax++;
            }

            AddAlertIfNeeded(currentThreadsMax, maxThreadsMax, MaxThreadsMaxThreshold, threads_max, nameof(threads_max));
        }

        private void CheckMaxMapCountLimits()
        {
            var maxMapCount = GetMaxValueForLimit(max_map_count);

            long currentMapCount = GetCurrentMapCount();
            AddAlertIfNeeded(currentMapCount, maxMapCount, MaxMapCountThreshold, max_map_count, nameof(max_map_count));
        }

        private static long GetCurrentMapCount()
        {
            long currentMapCount;
            using (FileStream stream = File.OpenRead("/proc/self/maps"))
            {
                currentMapCount = GetEolCount(stream);
            }

            return currentMapCount;
        }

        private long GetMaxValueForLimit(string limit)
        {
            if (_maxLimitsDictionary.TryGetValue(limit, out var maxValueForLimit) == false)
            {
                var maxValueString = File.ReadAllText(limit);
                if (long.TryParse(maxValueString, out var maxValueLong) && maxValueLong > 0)
                {
                    _maxLimitsDictionary.Add(limit, maxValueLong);
                    return maxValueLong;
                }

                throw new InvalidOperationException($"Could not parse the value of '{limit}', got: '{maxValueString}' and '{maxValueForLimit}'.");
            }

            return maxValueForLimit;
        }

        private void AddAlertIfNeeded(long currentMax, long maxMax, float threshold, string limit, string name)
        {
            if (currentMax >= (long)(maxMax * (1 - threshold)))
            {
                _alerts.Add($"'{name}' is ({currentMax} / {maxMax}), please increase the '{limit}' limit.");
            }
        }

        private static long GetEolCount(Stream stream)
        {
            long eolCount = 0L;
            char cr = '\r';
            char lf = '\n';
            char NULL = (char)0;

            byte[] byteBuffer = new byte[256 * 1024];
            char detectedEOL = NULL;
            char currentChar = NULL;

            int bytesRead;
            while ((bytesRead = stream.Read(byteBuffer, 0, byteBuffer.Length)) > 0)
            {
                for (int i = 0; i < bytesRead; i++)
                {
                    currentChar = (char)byteBuffer[i];

                    if (detectedEOL != NULL)
                    {
                        if (currentChar == detectedEOL)
                        {
                            eolCount++;
                        }
                    }
                    else if (currentChar == lf || currentChar == cr)
                    {
                        detectedEOL = currentChar;
                        eolCount++;
                    }
                }
            }

            if (currentChar != lf && currentChar != cr && currentChar != NULL)
            {
                eolCount++;
            }

            return eolCount;
        }

        public void Dispose()
        {
            _timer?.Dispose();
            _timer = null;
        }
    }
}
