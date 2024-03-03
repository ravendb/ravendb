using System;
using System.Threading.Tasks;

namespace Sparrow.Server.Utils.DiskStatsGetter;

public interface IDiskStatsGetter : IDisposable
{
    DiskStatsResult Get(string drive);
    Task<DiskStatsResult> GetAsync(string drive);
}
