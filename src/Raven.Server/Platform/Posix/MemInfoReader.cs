using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Reflection;
using Raven.Server.Monitoring.Snmp;
using Sparrow;
using Sparrow.Platform;

namespace Raven.Server.Platform.Posix
{
    public static class MemInfoReader
    {
        internal const string MemInfoFileName = "/proc/meminfo";

        private static readonly char[] Separators = { ' ' };

        public static MemInfo Read()
        {
            if (PlatformDetails.RunningOnPosix == false)
                throw new InvalidOperationException($"Cannot read '{MemInfoFileName}' because it requires POSIX");

            using (var fs = new FileStream(MemInfoFileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                return Read(fs);
        }

        public static MemInfo Read(Stream stream)
        {
            if (stream == null)
                throw new ArgumentNullException(nameof(stream));

            using (var reader = new StreamReader(stream))
            {
                var result = new MemInfo();

                while (true)
                {
                    string line = reader.ReadLine();
                    if (string.IsNullOrWhiteSpace(line))
                        break;

                    string[] values = line.Split(Separators, StringSplitOptions.RemoveEmptyEntries);

                    if (values.Length < 2 || values.Length > 3)
                        throw new InvalidOperationException($"Invalid line in '{MemInfoFileName}'. Line: '{line}'.");

                    string name = values[0].TrimEnd(':');
                    string valueAsString = values[1];
                    if (long.TryParse(valueAsString, out long value) == false)
                        throw new InvalidOperationException($"Invalid value in '{MemInfoFileName}'. Line: '{line}'. Value: '{valueAsString}'.");

                    SizeUnit unit = SizeUnit.Bytes;

                    if (values.Length == 3)
                    {
                        string unitAsString = values[2];
                        unit = unitAsString switch
                        {
                            "kB" => SizeUnit.Kilobytes,
                            _ => throw new InvalidOperationException($"Invalid unit in '{MemInfoFileName}'. Line: '{line}'. Unit: '{unitAsString}'.")
                        };
                    }

                    result.Set(name, value, unit);
                }

                return result;
            }
        }
    }

    public class MemInfo
    {
        public static readonly Dictionary<string, PropertyInfo> AllProperties = new Dictionary<string, PropertyInfo>();

        public static readonly MemInfo Invalid = new MemInfo();

        static MemInfo()
        {
            var indexes = new HashSet<int>();
            foreach (PropertyInfo property in typeof(MemInfo).GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (property.Name == nameof(Other))
                    continue;

                var index = GetPropertySnmpIndex(property);
                if (indexes.Add(index) == false)
                    throw new InvalidOperationException($"{nameof(MemInfo)} property '{property.Name}' must have a unique SNMP index.");

                AllProperties[NormalizePropertyName(property)] = property;
            }

            static string NormalizePropertyName(MemberInfo memberInfo)
            {
                var displayNameAttribute = memberInfo.GetCustomAttribute<DisplayNameAttribute>();
                if (displayNameAttribute == null)
                    return memberInfo.Name;

                return displayNameAttribute.DisplayName;
            }

            static int GetPropertySnmpIndex(MemberInfo memberInfo)
            {
                var snmpIndexAttribute = memberInfo.GetCustomAttribute<SnmpIndexAttribute>();
                if (snmpIndexAttribute == null || snmpIndexAttribute.Index <= 0)
                    throw new InvalidOperationException($"{nameof(MemInfo)} property '{memberInfo.Name}' must have {nameof(SnmpIndexAttribute)}");

                return snmpIndexAttribute.Index;
            }
        }

        public MemInfo()
        {
            Other = new Dictionary<string, Size>();
        }

        [SnmpIndex(1)]
        public Size MemTotal { get; set; }

        [SnmpIndex(2)]
        public Size MemFree { get; set; }

        [SnmpIndex(3)]
        public Size Buffers { get; set; }

        [SnmpIndex(4)]
        public Size Cached { get; set; }

        [SnmpIndex(5)]
        public Size SwapCached { get; set; }

        [SnmpIndex(6)]
        public Size Active { get; set; }

        [SnmpIndex(7)]
        public Size Inactive { get; set; }

        [SnmpIndex(8)]
        [DisplayName("Active(anon)")]
        public Size Active_anon { get; set; }

        [SnmpIndex(9)]
        [DisplayName("Inactive(anon)")]
        public Size Inactive_anon { get; set; }

        [SnmpIndex(10)]
        [DisplayName("Active(file)")]
        public Size Active_file { get; set; }

        [SnmpIndex(11)]
        [DisplayName("Inactive(file)")]
        public Size Inactive_file { get; set; }

        [SnmpIndex(12)]
        public Size Unevictable { get; set; }

        [SnmpIndex(13)]
        public Size Mlocked { get; set; }

        [SnmpIndex(14)]
        public Size SwapTotal { get; set; }

        [SnmpIndex(15)]
        public Size SwapFree { get; set; }

        [SnmpIndex(16)]
        public Size Dirty { get; set; }

        [SnmpIndex(17)]
        public Size Writeback { get; set; }

        [SnmpIndex(18)]
        public Size AnonPages { get; set; }

        [SnmpIndex(19)]
        public Size Mapped { get; set; }

        [SnmpIndex(20)]
        public Size Shmem { get; set; }

        [SnmpIndex(21)]
        public Size Slab { get; set; }

        [SnmpIndex(22)]
        public Size SReclaimable { get; set; }

        [SnmpIndex(23)]
        public Size SUnreclaim { get; set; }

        [SnmpIndex(24)]
        public Size KernelStack { get; set; }

        [SnmpIndex(25)]
        public Size PageTables { get; set; }

        [SnmpIndex(26)]
        public Size NFS_Unstable { get; set; }

        [SnmpIndex(27)]
        public Size Bounce { get; set; }

        [SnmpIndex(28)]
        public Size WritebackTmp { get; set; }

        [SnmpIndex(29)]
        public Size CommitLimit { get; set; }

        [SnmpIndex(30)]
        public Size Committed_AS { get; set; }

        [SnmpIndex(31)]
        public Size VmallocTotal { get; set; }

        [SnmpIndex(32)]
        public Size VmallocUsed { get; set; }

        [SnmpIndex(33)]
        public Size VmallocChunk { get; set; }

        [SnmpIndex(34)]
        public Size HardwareCorrupted { get; set; }

        [SnmpIndex(35)]
        public Size AnonHugePages { get; set; }

        [SnmpIndex(36)]
        public Size HugePages_Total { get; set; }

        [SnmpIndex(37)]
        public Size HugePages_Free { get; set; }

        [SnmpIndex(38)]
        public Size HugePages_Rsvd { get; set; }

        [SnmpIndex(39)]
        public Size HugePages_Surp { get; set; }

        [SnmpIndex(40)]
        public Size Hugepagesize { get; set; }

        [SnmpIndex(41)]
        public Size DirectMap4k { get; set; }

        [SnmpIndex(42)]
        public Size DirectMap4M { get; set; }

        [SnmpIndex(43)]
        public Size MemAvailable { get; set; }

        [SnmpIndex(44)]
        public Size KReclaimable { get; set; }

        [SnmpIndex(45)]
        public Size Percpu { get; set; }

        [SnmpIndex(46)]
        public Size ShmemHugePages { get; set; }

        [SnmpIndex(47)]
        public Size ShmemPmdMapped { get; set; }

        [SnmpIndex(48)]
        public Size FileHugePages { get; set; }

        [SnmpIndex(49)]
        public Size FilePmdMapped { get; set; }

        [SnmpIndex(50)]
        public Size Hugetlb { get; set; }

        [SnmpIndex(51)]
        public Size DirectMap2M { get; set; }

        [SnmpIndex(52)]
        public Size DirectMap1G { get; set; }

        [SnmpIndex(53)]
        public Size CmaTotal { get; set; }

        [SnmpIndex(54)]
        public Size CmaFree { get; set; }

        [SnmpIndex(55)]
        public Size Zswap { get; set; }

        [SnmpIndex(56)]
        public Size Zswapped { get; set; }

        [SnmpIndex(57)]
        public Size SecPageTables { get; set; }

        [SnmpIndex(58)]
        public Size Unaccepted { get; set; }

        public Dictionary<string, Size> Other { get; set; }

        public void Set(string name, long value, SizeUnit unit)
        {
            var size = new Size(value, unit);

            if (AllProperties.TryGetValue(name, out PropertyInfo property))
            {
                property.SetValue(this, size);
                return;
            }

            Other[name] = size;
        }
    }
}
