using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Reflection;
using Sparrow.Platform;

namespace Sparrow.Server.Platform.Posix
{
    public static class MemInfoReader
    {
        private const string MemInfoFileName = "/proc/meminfo";

        private static readonly char[] Separators = { ' ' };

        public static MemInfo Read()
        {
            if (PlatformDetails.RunningOnPosix == false)
                throw new InvalidOperationException($"Cannot read '{MemInfoFileName}' because it requires POSIX");

            using (FileStream fs = new(MemInfoFileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                return Read(fs);
        }

        public static MemInfo Read(Stream stream)
        {
            if (stream == null)
                throw new ArgumentNullException(nameof(stream));

            using (StreamReader reader = new(stream))
            {
                MemInfo result = new();

                while (true)
                {
                    string line = reader.ReadLine();
                    if (string.IsNullOrWhiteSpace(line))
                        break;

                    string[] values = line.Split(Separators, StringSplitOptions.RemoveEmptyEntries);

                    if (values.Length is < 2 or > 3)
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
        private static readonly Dictionary<string, PropertyInfo> Properties = new();

        static MemInfo()
        {
            foreach (PropertyInfo property in typeof(MemInfo).GetProperties(BindingFlags.Public | BindingFlags.Instance))
                Properties[NormalizeName(property)] = property;

            static string NormalizeName(MemberInfo propertyInfo)
            {
                var displayNameAttribute = propertyInfo.GetCustomAttribute<DisplayNameAttribute>();
                if (displayNameAttribute == null)
                    return propertyInfo.Name;

                return displayNameAttribute.DisplayName;
            }
        }

        public MemInfo()
        {
            Other = new Dictionary<string, Size>();
        }

        public Size MemTotal { get; set; }
        public Size MemFree { get; set; }
        public Size Buffers { get; set; }
        public Size Cached { get; set; }
        public Size SwapCached { get; set; }
        public Size Active { get; set; }
        public Size Inactive { get; set; }

        [DisplayName("Active(anon)")]
        public Size Active_anon { get; set; }

        [DisplayName("Inactive(anon)")]
        public Size Inactive_anon { get; set; }

        [DisplayName("Active(file)")]
        public Size Active_file { get; set; }

        [DisplayName("Inactive(file)")]
        public Size Inactive_file { get; set; }

        public Size Unevictable { get; set; }
        public Size Mlocked { get; set; }
        public Size SwapTotal { get; set; }
        public Size SwapFree { get; set; }
        public Size Dirty { get; set; }
        public Size Writeback { get; set; }
        public Size AnonPages { get; set; }
        public Size Mapped { get; set; }
        public Size Shmem { get; set; }
        public Size Slab { get; set; }
        public Size SReclaimable { get; set; }
        public Size SUnreclaim { get; set; }
        public Size KernelStack { get; set; }
        public Size PageTables { get; set; }
        public Size NFS_Unstable { get; set; }
        public Size Bounce { get; set; }
        public Size WritebackTmp { get; set; }
        public Size CommitLimit { get; set; }
        public Size Committed_AS { get; set; }
        public Size VmallocTotal { get; set; }
        public Size VmallocUsed { get; set; }
        public Size VmallocChunk { get; set; }
        public Size HardwareCorrupted { get; set; }
        public Size AnonHugePages { get; set; }
        public Size HugePages_Total { get; set; }
        public Size HugePages_Free { get; set; }
        public Size HugePages_Rsvd { get; set; }
        public Size HugePages_Surp { get; set; }
        public Size Hugepagesize { get; set; }
        public Size DirectMap4k { get; set; }
        public Size DirectMap4M { get; set; }
        public Dictionary<string, Size> Other { get; set; }

        public void Set(string name, long value, SizeUnit unit)
        {
            Size size = new(value, unit);

            if (Properties.TryGetValue(name, out PropertyInfo property))
            {
                property.SetValue(this, size);
                return;
            }

            Other[name] = size;
        }
    }
}
