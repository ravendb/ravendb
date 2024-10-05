using System;
using System.Buffers.Text;
using System.IO;
using System.Runtime.Versioning;
using System.Text;
using Mono.Unix.Native;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Sparrow.Server.Utils.DiskStatsGetter;

[SupportedOSPlatform("linux")]
internal class LinuxDiskStatsGetter : DiskStatsGetter<LinuxDiskStatsRawResult>
{
    private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForSparrowServer(typeof(LinuxDiskStatsGetter));

    public LinuxDiskStatsGetter(TimeSpan minInterval) : base(minInterval)
    {
    }

    protected override DiskStatsResult CalculateStats(LinuxDiskStatsRawResult currentInfo, State state)
    {
        var diff = (currentInfo.Time - state.Result.RawSampling.Time).TotalSeconds;
        var diskSpaceResult = new DiskStatsResult
        {
            IoReadOperations = (currentInfo.IoReadOperations - state.Result.RawSampling.IoReadOperations) / diff,
            IoWriteOperations = (currentInfo.IoWriteOperations - state.Result.RawSampling.IoWriteOperations) / diff,
            ReadThroughput = new Size((long)((currentInfo.ReadSectors - state.Result.RawSampling.ReadSectors) * 512 / diff), SizeUnit.Bytes),
            WriteThroughput = new Size((long)((currentInfo.WriteSectors - state.Result.RawSampling.WriteSectors) * 512 / diff), SizeUnit.Bytes),
            QueueLength = currentInfo.QueueLength
        };
        return diskSpaceResult;
    }

    protected override LinuxDiskStatsRawResult GetDiskInfo(string path)
    {
        try
        {
            var (major, minor) = GetDiskMajorMinor(path);

            return ReadParse($"/sys/dev/block/{major}:{minor}/stat");
        }
        catch (Exception e)
        {
            if (Logger.IsWarnEnabled)
                Logger.Warn($"Could not get GetDiskInfo of {path}", e);
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
            Syscall.strerror_r(errno, errorBuilder, bufferCapacity);
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

    private static LinuxDiskStatsRawResult ReadParse(string statPath)
    {
        const int maxNumberOfValues = 11;
        Span<long> values = stackalloc long[maxNumberOfValues];
        Span<byte> buffer = stackalloc byte[1024];

        var readTime = DateTime.UtcNow;
        var read = ReadAllInto(statPath, buffer);
        var contents = buffer[..read];

        var numberOfValues = Parse(contents, values);

        /*
         *https://www.kernel.org/doc/Documentation/block/stat.txt
         *https://github.com/sysstat/sysstat/blob/master/iostat.c#L429
         */
        int ioWriteOperationsIndex;
        int readSectorsIndex;
        int writeSectorsIndex;
        long? queueLength = null;
        if (numberOfValues >= maxNumberOfValues)
        {
            /* Device or partition */
            ioWriteOperationsIndex = 4;
            readSectorsIndex = 2;
            writeSectorsIndex = 6;
            queueLength = values[8];
        }
        else if (numberOfValues == 4)
        {
            /* Partition without extended statistics */
            ioWriteOperationsIndex = 2;
            readSectorsIndex = 1;
            writeSectorsIndex = 3;
        }
        else
        {
            if (Logger.IsDebugEnabled)
                Logger.Debug($"The stats file {statPath} should contain at least 4 values. File content '{Encoding.UTF8.GetString(contents)}'");
            return null;
        }

        return new LinuxDiskStatsRawResult
        {
            IoReadOperations = values[0],
            IoWriteOperations = values[ioWriteOperationsIndex],
            ReadSectors = values[readSectorsIndex],
            WriteSectors = values[writeSectorsIndex],
            QueueLength = queueLength,
            Time = readTime
        };
    }

    private static int ReadAllInto(string path, Span<byte> remainedToRead)
    {
        using var stream = File.OpenRead(path);
        int totalRead = 0;
        while (true)
        {
            int read = stream.Read(remainedToRead);
            remainedToRead = remainedToRead[read..];
            if (read == 0)
                return totalRead;
            totalRead += read;
        }
    }

    private static int Parse(Span<byte> content, Span<long> values)
    {
        int valuesIndex = 0;
        while (content.IsEmpty == false && valuesIndex < values.Length)
        {
            var position = 0;
            while (content[position] == (byte)' ')
            {
                position++;
                if (position >= content.Length)
                    return valuesIndex;
            }

            content = content[position..];
            if (Utf8Parser.TryParse(content, out long v, out var consumed) == false)
            {
                if (content[0] == '\n')
                    break;
                throw new InvalidOperationException($"Failed to parse {Encoding.UTF8.GetString(content)} to number");
            }

            content = content[consumed..];
            values[valuesIndex++] = v;
        }

        return valuesIndex;
    }

    public override void Dispose()
    {

    }
}
