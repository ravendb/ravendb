using System;
using System.Buffers.Text;
using System.IO;
using System.Runtime.Versioning;
using System.Text;
using Mono.Unix.Native;
using Sparrow.Logging;

namespace Sparrow.Server.Utils.DiskStatsGetter;

[SupportedOSPlatform("linux")]
internal class LinuxDiskStatsGetter : DiskStatsGetter<LinuxDiskStatsRawResult>
{
    private static readonly Logger Logger = LoggingSource.Instance.GetLogger<LinuxDiskStatsGetter>("Server");

    public LinuxDiskStatsGetter(TimeSpan minInterval) : base(minInterval)
    {
    }

    protected override DiskStatsResult CalculateStats(LinuxDiskStatsRawResult currentInfo, State state)
    {
        var diff = (currentInfo.Time - state.Prev.Raw.Time).TotalSeconds;
        var diskSpaceResult = new DiskStatsResult
        {
            IoReadOperations = (currentInfo.IoReadOperations - state.Prev.Raw.IoReadOperations) / diff, 
            IoWriteOperations = (currentInfo.IoWriteOperations - state.Prev.Raw.IoWriteOperations) / diff, 
            ReadThroughput = new Size((long)((currentInfo.ReadSectors - state.Prev.Raw.ReadSectors) * 512 / diff), SizeUnit.Bytes), 
            WriteThroughput = new Size((long)((currentInfo.WriteSectors - state.Prev.Raw.WriteSectors) * 512 / diff), SizeUnit.Bytes), 
            QueueLength = currentInfo.QueueLength
        };
        return diskSpaceResult;
    }

    protected override LinuxDiskStatsRawResult GetDiskInfo(string path)
    {
        try
        {
            var (major, minor) = GetDiskMajorMinor(path);

            var statPath = $"/sys/dev/block/{major}:{minor}/stat";
            Span<byte> buf = stackalloc byte[1024];
 
            var time = DateTime.UtcNow;
            using (var reader = File.OpenRead(statPath))
            {
                var read = reader.Read(buf);
                buf = buf.Slice(0, read);
            }
            
            return Parse(buf, time, statPath);
        }
        catch (Exception e)
        {
            if(Logger.IsInfoEnabled)
                Logger.Info($"Could not get GetDiskInfo of {path}", e);
            return null;
        }
    }
        
    private static (ulong Major, ulong Minor) GetDiskMajorMinor(string path)
    {
        if (Syscall.stat(path, out var stats) != 0)
        {
            const int bufferCapacity = 1024;
            var errno = Stdlib.GetLastError();
            var errorBuilder = new StringBuilder(bufferCapacity);
            Syscall.strerror_r(errno, errorBuilder,bufferCapacity);
            errorBuilder.Insert(0, $"Failed to get stat for \"{path}\" : ");
            throw new InvalidOperationException(errorBuilder.ToString());
        }
            
        var deviceId = (stats.st_mode & FilePermissions.S_IFBLK) == FilePermissions.S_IFBLK
            ? stats.st_rdev
            : stats.st_dev;
                
        //https://sites.uclouvain.be/SystInfo/usr/include/sys/sysmacros.h.html
        var major = (deviceId & 0x00000000000fff00u) >> 8;
        major |= (deviceId & 0xfffff00000000000u) >> 32;
                
        var minor = (deviceId & 0x00000000000000ffu);
        minor |= (deviceId & 0x00000ffffff00000u) >> 12;
        return (major, minor);
    }
     
    private static LinuxDiskStatsRawResult Parse(Span<byte> content, DateTime time, string statPath)
    {
        const int maxValuesLength = 17;

        Span<long> values = stackalloc long[maxValuesLength];
        int numberOfValues = Parse(content, values);
        /*
         *https://www.kernel.org/doc/Documentation/block/stat.txt
         *https://github.com/sysstat/sysstat/blob/master/iostat.c#L429
         */
        int ioWriteOperationsIndex;
        int readSectorsIndex;
        int writeSectorsIndex;
        long? queueLength = null;
        if (numberOfValues >= 11) {
            /* Device or partition */
            ioWriteOperationsIndex = 4;
            readSectorsIndex = 2;
            writeSectorsIndex = 6;
            queueLength = values[8];
        }
        else if (numberOfValues == 4) {
            /* Partition without extended statistics */
            ioWriteOperationsIndex = 2;
            readSectorsIndex = 1;
            writeSectorsIndex = 3;
        }
        else
        {
            if(Logger.IsInfoEnabled)
                Logger.Info($"The stats file {statPath} should contain at least 4 values. File content '{Encoding.UTF8.GetString(content)}'");
            return null;
        }

        return new LinuxDiskStatsRawResult
        {
            IoReadOperations = values[0], 
            IoWriteOperations = values[ioWriteOperationsIndex],
            ReadSectors = values[readSectorsIndex],
            WriteSectors = values[writeSectorsIndex],
            QueueLength = queueLength,
                
            Time = time
        };
    }

    private static int Parse(Span<byte> content, Span<long> values)
    {
        int valuesIndex = 0;
        while (content.Length > 0)
        {
            var position = 0;
            while (char.IsWhiteSpace((char)content[position]))
            {
                position++;
                if (position >= content.Length)
                    return valuesIndex;
            }
            content = content.Slice(position);

            if (Utf8Parser.TryParse(content, out long value, out var bytesConsumed) == false)
                throw new InvalidOperationException($"Failed to parse {Encoding.UTF8.GetString(content)} to number");
            
            values[valuesIndex++] = value;
            content = content.Slice(bytesConsumed);
        }

        return valuesIndex;
    }
}
