using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Raven.Server.Logging;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server.Logging;

namespace Raven.Server.Utils
{
    internal sealed class ThreadIoStatsReader : IDisposable
    {
        private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer<ThreadIoStatsReader>();
        private readonly ConcurrentDictionary<int, Stats> _stats = new ConcurrentDictionary<int, Stats>();
        private readonly ConcurrentDictionary<int, PrevSample> _prevSamples = new ConcurrentDictionary<int, PrevSample>();
        private Timer _timer;
        private int _collecting;

        public sealed class Stats
        {
            public double IoSyscallsPerSecLast;
            public double KbPerSecLast;

            // Split metrics when available
            public double? ReadIoSyscallsPerSecLast;
            public double? WriteIoSyscallsPerSecLast;
            public double? ReadKbPerSecLast;
            public double? WriteKbPerSecLast;

            // Raw cumulative values from /proc
            public long Syscr;
            public long Syscw;
            public long ReadBytes;
            public long WriteBytes;
        }

        private ThreadIoStatsReader()
        {
            if (PlatformDetails.RunningOnLinux == false)
                return;

            try
            {
                _timer = new Timer(_ =>
                {
                    if (Interlocked.Exchange(ref _collecting, 1) != 0)
                        return;
                    try
                    {
                        CollectOnce();
                    }
                    catch (Exception e)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info("Error in thread IO stats collector.", e);
                    }
                    finally
                    {
                        Volatile.Write(ref _collecting, 0);
                    }
                }, null, TimeSpan.Zero, TimeSpan.FromSeconds(1));
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failed to start timer for thread IO stats collector. Thread IO stats disabled.", e);
            }
        }

        private sealed class PrevSample
        {
            public long Syscr;
            public long Syscw;
            public long ReadBytes;
            public long WriteBytes;
            public DateTime Last;
        }


        private static ThreadIoStatsReader _instance;
        private static readonly object InstanceLock = new object();

        public static ThreadIoStatsReader Instance
        {
            get
            {
                if (_instance != null)
                    return _instance;
                lock (InstanceLock)
                {
                    return _instance ??= new ThreadIoStatsReader();
                }
            }
        }

        public static void DisposeInstance()
        {
            lock (InstanceLock)
            {
                try
                {
                    _instance?.Dispose();
                }
                catch
                {
                    // ignore
                }
                finally
                {
                    _instance = null;
                }
            }
        }

        public bool TryGet(int tid, out Stats stats) => _stats.TryGetValue(tid, out stats);

        public void Dispose()
        {
            try
            {
                _timer?.Dispose();
                _timer = null;
            }
            catch
            {
                // ignore
            }
            
            _stats.Clear();
            _prevSamples.Clear();
        }

        private void CollectOnce()
        {
            const double Kb = 1024.0;
            var taskDir = "/proc/self/task";

            try
            {
                var seen = new HashSet<int>();
                string[] taskSubDirs = Array.Empty<string>();
                try
                {
                    if (Directory.Exists(taskDir))
                        taskSubDirs = Directory.GetDirectories(taskDir);
                }
                catch
                {
                    // ignore
                }

                foreach (var dir in taskSubDirs)
                {
                    int tid;
                    try
                    {
                        var name = Path.GetFileName(dir);
                        if (int.TryParse(name, out tid) == false)
                            continue;
                    }
                    catch
                    {
                        continue;
                    }

                    seen.Add(tid);

                    var ioPath = Path.Combine(dir, "io");
                    string[] lines;
                    try
                    {
                        lines = File.ReadAllLines(ioPath);
                    }
                    catch
                    {
                        // thread might have exited
                        continue;
                    }

                    long syscr = 0, syscw = 0, readBytes = 0, writeBytes = 0;
                    foreach (var line in lines)
                    {
                        if (line.StartsWith("syscr:"))
                            long.TryParse(line.AsSpan(6).Trim(), out syscr);
                        else if (line.StartsWith("syscw:"))
                            long.TryParse(line.AsSpan(6).Trim(), out syscw);
                        else if (line.StartsWith("read_bytes:"))
                            long.TryParse(line.AsSpan(11).Trim(), out readBytes);
                        else if (line.StartsWith("write_bytes:"))
                            long.TryParse(line.AsSpan(12).Trim(), out writeBytes);
                    }

                    var now = DateTime.UtcNow;
                    _prevSamples.AddOrUpdate(tid, new PrevSample
                    {
                        Syscr = syscr,
                        Syscw = syscw,
                        ReadBytes = readBytes,
                        WriteBytes = writeBytes,
                        Last = now
                    }, (k, prev) =>
                    {
                        var elapsed = (now - prev.Last).TotalSeconds;
                        if (elapsed <= 0)
                            elapsed = 1;

                        var dReadOps = Math.Max(0, syscr - prev.Syscr);
                        var dWriteOps = Math.Max(0, syscw - prev.Syscw);
                        var dReadBytes = Math.Max(0L, readBytes - prev.ReadBytes);
                        var dWriteBytes = Math.Max(0L, writeBytes - prev.WriteBytes);

                        var readOpsPerSec = dReadOps / elapsed;
                        var writeOpsPerSec = dWriteOps / elapsed;
                        var readKbPerSec = dReadBytes / Kb / elapsed;
                        var writeKbPerSec = dWriteBytes / Kb / elapsed;

                        _stats.AddOrUpdate(tid, new Stats
                        {
                            IoSyscallsPerSecLast = readOpsPerSec + writeOpsPerSec,
                            KbPerSecLast = readKbPerSec + writeKbPerSec,
                            ReadIoSyscallsPerSecLast = readOpsPerSec,
                            WriteIoSyscallsPerSecLast = writeOpsPerSec,
                            ReadKbPerSecLast = readKbPerSec,
                            WriteKbPerSecLast = writeKbPerSec,
                            Syscr = syscr,
                            Syscw = syscw,
                            ReadBytes = readBytes,
                            WriteBytes = writeBytes
                        }, (kk, s) =>
                        {
                            s.IoSyscallsPerSecLast = readOpsPerSec + writeOpsPerSec;
                            s.KbPerSecLast = readKbPerSec + writeKbPerSec;
                            s.ReadIoSyscallsPerSecLast = readOpsPerSec;
                            s.WriteIoSyscallsPerSecLast = writeOpsPerSec;
                            s.ReadKbPerSecLast = readKbPerSec;
                            s.WriteKbPerSecLast = writeKbPerSec;
                            s.Syscr = syscr;
                            s.Syscw = syscw;
                            s.ReadBytes = readBytes;
                            s.WriteBytes = writeBytes;
                            return s;
                        });

                        prev.Syscr = syscr;
                        prev.Syscw = syscw;
                        prev.ReadBytes = readBytes;
                        prev.WriteBytes = writeBytes;
                        prev.Last = now;
                        return prev;
                    });
                }

                // cleanup disappeared threads
                try
                {
                    foreach (var key in _prevSamples.Keys)
                    {
                        if (seen.Contains(key) == false)
                            _prevSamples.TryRemove(key, out _);
                    }
                }
                catch
                {
                    // ignore
                }
            }
            catch (Exception ex)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Error while collecting thread IO stats.", ex);
            }
        }
    }
}
