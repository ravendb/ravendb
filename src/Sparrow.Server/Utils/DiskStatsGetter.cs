using System;
using System.Collections.Generic;
using Sparrow.Logging;
using Sparrow.Server.Platform;

namespace Sparrow.Server.Utils
{
    public interface IDiskStatsGetter
    {
        DiskStatsResult Get(string drive);
    }

    internal class DiskStatsGetter : IDiskStatsGetter
    {
        private readonly double _minInterval;
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger("Server", typeof(DiskStatsGetter).FullName);

        private readonly object _locker = new object();
        private readonly Dictionary<string, PrevInfo> _previousInfo = new Dictionary<string, PrevInfo>();

        public DiskStatsGetter(TimeSpan minInterval)
        {
            _minInterval = minInterval.TotalSeconds;
        }
        
        public DiskStatsResult Get(string drive)
        {
            if (drive == null)
                return null;

            lock (_locker)
            {
                if (_previousInfo.TryGetValue(drive, out var prev))
                {
                    var diff = (DateTime.UtcNow - prev.Raw.Time).TotalSeconds;
                    if (diff < _minInterval)
                        return prev.Calculated;

                    var currentInfo = GetDiskInfo(drive);
                    if (currentInfo == null)
                        return null;
                
                    var read = (currentInfo.ReadIOs - prev.Raw.ReadIOs) / diff;
                    var write = (currentInfo.WriteIOs - prev.Raw.WriteIOs) / diff;
                    var diskSpaceResult = new DiskStatsResult {ReadIos = read, WriteIos = write};
                
                    prev.Raw = currentInfo;
                    prev.Calculated = diskSpaceResult;
                    return diskSpaceResult;
                }
                else
                {
                    var currentInfo = GetDiskInfo(drive);
                    if (currentInfo != null)
                        _previousInfo[drive] = new PrevInfo {Raw = currentInfo};
                    return null;
                }
            }
        }

        private DiskStatsRawResult GetDiskInfo(string path)
        {
            var result = Pal.rvn_get_path_disk_stats(path, out var ioStats, out var error);
            if (result == PalFlags.FailCodes.Success)
                return new DiskStatsRawResult {Time = DateTime.UtcNow, ReadIOs = (long)ioStats.Read, WriteIOs = (long)ioStats.Write};
            
            if (Logger.IsInfoEnabled)
                Logger.Info(PalHelper.GetNativeErrorString(error, $"Failed to get file system statistics for path: {path}. FailCode={result}", out _));
         
            return null;
        }
        
        private record DiskStatsRawResult
        {
            public long ReadIOs { get; init; }
    
            public long WriteIOs { get; init; }
    
            public DateTime Time { get; init;}
        }

        class PrevInfo
        {
            public DiskStatsResult Calculated { get; set; }
            public DiskStatsRawResult Raw { get; set; }
        }
    }
    
    public record DiskStatsResult
    {
        public double ReadIos { get; init; }
    
        public double WriteIos { get; init;}
    }

    internal class NotImplementedDiskStatsGetter : IDiskStatsGetter
    {
        public DiskStatsResult Get(string drive) => null;
    }    
}
