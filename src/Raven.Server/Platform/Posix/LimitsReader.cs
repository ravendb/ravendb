using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.IO.Pipelines;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Monitoring.Snmp;
using Sparrow.Platform;

namespace Raven.Server.Platform.Posix
{
    public static class LimitsReader
    {
        public static readonly string MaxMapCountFilePath = "/proc/sys/vm/max_map_count";
        public static readonly string CurrentMapCountFilePath = "/proc/self/maps";

        public static readonly string ThreadsMaxFilePath = "/proc/sys/kernel/threads-max";
        public static readonly string ThreadsCurrentFilePath = "/proc/self/stat";

        private static readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);

        public static Task<LimitsInfo> ReadCurrentLimitsAsync()
        {
            if (PlatformDetails.RunningOnLinux)
                return ReadCurrentLimitsForLinuxAsync();

            if (PlatformDetails.RunningOnWindows)
                return ReadCurrentLimitsForWindowsAsync();

            throw new NotSupportedException($"Current OS '{RuntimeInformation.OSDescription}' is not supported.");
        }

        private static async Task<LimitsInfo> ReadCurrentLimitsForWindowsAsync()
        {
            if (PlatformDetails.RunningOnWindows == false)
                throw new InvalidOperationException("Cannot read Current Limits because it requires Windows");

            if (await _lock.WaitAsync(0) == false)
                return LimitsInfo.Current;

            try
            {
                using (var process = Process.GetCurrentProcess())
                {
                    LimitsInfo.Current.MapCountCurrent = -1;
                    LimitsInfo.Current.ThreadsCurrent = process.Threads.Count;
                    LimitsInfo.Current.SetValues();
                }
            }
            finally
            {
                _lock.Release();
            }

            return LimitsInfo.Current;
        }

        private static async Task<LimitsInfo> ReadCurrentLimitsForLinuxAsync()
        {
            if (PlatformDetails.RunningOnPosix == false)
                throw new InvalidOperationException("Cannot read Current Limits because it requires POSIX");

            if (await _lock.WaitAsync(0) == false)
                return LimitsInfo.Current;

            try
            {
                LimitsInfo.Current.MapCountCurrent = await GetCurrentMapCountAsync();
                LimitsInfo.Current.ThreadsCurrent = GetCurrentThreadsCount();
                LimitsInfo.Current.SetValues();
            }
            finally
            {
                _lock.Release();
            }

            return LimitsInfo.Current;
        }

        public static LimitsInfo ReadMaxLimits()
        {
            if (PlatformDetails.RunningOnPosix == false)
                throw new InvalidOperationException("Cannot read Current Limits because it requires POSIX");

            _lock.Wait();

            try
            {
                LimitsInfo.Current.MapCountMax = GetMaxMapCount();
                LimitsInfo.Current.ThreadsMax = GetMaxThreadsCount();
                LimitsInfo.Current.SetValues();
            }
            finally
            {
                _lock.Release();
            }

            return LimitsInfo.Current;
        }

        private static long GetMaxValueForLimit(string limit)
        {
            var maxValueString = File.ReadAllText(limit);
            if (long.TryParse(maxValueString, out var maxValueLong) && maxValueLong > 0)
            {
                return maxValueLong;
            }

            throw new InvalidOperationException($"Could not parse the value of '{limit}', got: '{maxValueString}'.");
        }

        private static long GetMaxThreadsCount()
        {
            return GetMaxValueForLimit(ThreadsMaxFilePath);
        }

        private static long GetMaxMapCount()
        {
            return GetMaxValueForLimit(MaxMapCountFilePath);
        }

        private static long GetCurrentThreadsCount()
        {
            var arrString = File.ReadAllText(ThreadsCurrentFilePath);
            var arr = arrString.Split(' ');

            var maxValueString = arr[19]; // num_threads  %ld

            if (long.TryParse(maxValueString, out var maxValueLong) && maxValueLong > 0)
            {
                return maxValueLong;
            }

            throw new InvalidOperationException($"Could not parse the value of '{ThreadsCurrentFilePath}', got: '{maxValueString}'.");
        }

        private static async Task<long> GetCurrentMapCountAsync()
        {
            long currentMapCount;
            await using (FileStream stream = File.OpenRead(CurrentMapCountFilePath))
            {
                currentMapCount = await GetEolCountAsync(stream);
            }

            return currentMapCount;
        }

        private static async Task<long> GetEolCountAsync(Stream stream)
        {
            var reader = PipeReader.Create(stream);
            long eolCount = 0L;
            while (true)
            {
                var result = await reader.ReadAsync();
                var buffer = result.Buffer;

                while (TryReadLine(ref buffer))
                {
                    // Process the line.
                    eolCount++;
                }

                // Tell the PipeReader how much of the buffer has been consumed.
                reader.AdvanceTo(buffer.Start, buffer.End);

                // Stop reading if there's no more data coming.
                if (result.IsCompleted)
                {
                    break;
                }
            }

            return eolCount;
        }

        private static bool TryReadLine(ref ReadOnlySequence<byte> buffer)
        {
            // Look for a EOL in the buffer.
            var position = buffer.PositionOf((byte)'\n');

            if (position == null)
            {
                return false;
            }

            buffer = buffer.Slice(buffer.GetPosition(1, position.Value));
            return true;
        }
    }

    public class LimitsInfo
    {
        [SnmpIndex(1)]
        [Description("Value of the '/proc/sys/vm/max_map_count' parameter")]
        public long MapCountMax { get; set; }

        [SnmpIndex(2)]
        [Description("Number of current map files in '/proc/self/maps'")]
        public long MapCountCurrent { get; set; }

        [SnmpIndex(3)]
        [Description("Value of the '/proc/sys/kernel/threads-max' parameter")]
        public long ThreadsMax { get; set; }

        [SnmpIndex(4)]
        [Description("Number of current threads")]
        public long ThreadsCurrent { get; set; }

        public static readonly ConcurrentDictionary<string, PropertyInfo> AllProperties = new ConcurrentDictionary<string, PropertyInfo>();
        public static readonly LimitsInfo Invalid = new LimitsInfo() { ThreadsMax = -1, ThreadsCurrent = -1, MapCountCurrent = -1, MapCountMax = -1 };

        public static readonly LimitsInfo Current = new LimitsInfo();

        static LimitsInfo()
        {
            var indexes = new HashSet<int>();
            foreach (PropertyInfo property in typeof(LimitsInfo).GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                var index = GetPropertySnmpIndex(property);
                if (indexes.Add(index) == false)
                    throw new InvalidOperationException($"{nameof(LimitsInfo)} property '{property.Name}' must have a unique SNMP index.");

                AllProperties[NormalizePropertyName(property)] = property;
            }

            static int GetPropertySnmpIndex(MemberInfo memberInfo)
            {
                var snmpIndexAttribute = memberInfo.GetCustomAttribute<SnmpIndexAttribute>();
                if (snmpIndexAttribute == null || snmpIndexAttribute.Index <= 0)
                    throw new InvalidOperationException($"{nameof(LimitsInfo)} property '{memberInfo.Name}' must have {nameof(SnmpIndexAttribute)}");

                return snmpIndexAttribute.Index;
            }
        }

        public void SetValues()
        {
            var t = typeof(LimitsInfo);
            foreach (var prop in t.GetProperties())
            {
                var obj = prop.GetValue(Current);
                if (obj == null)
                    continue;

                long res = (long)obj;
                if (res == default)
                    continue;

                Set(NormalizePropertyName(prop), res);
            }
        }

        public void Set(string name, long value)
        {
            if (AllProperties.TryGetValue(name, out PropertyInfo property))
            {
                property.SetValue(this, value);
            }
        }

        static string NormalizePropertyName(MemberInfo memberInfo)
        {
            var displayNameAttribute = memberInfo.GetCustomAttribute<DisplayNameAttribute>();
            if (displayNameAttribute == null)
                return memberInfo.Name;

            return displayNameAttribute.DisplayName;
        }
    }
}
