using System.Threading.Tasks;

namespace Sparrow.Server.Utils.DiskStatsGetter;

internal class NotImplementedDiskStatsGetter : IDiskStatsGetter
{
    public DiskStatsResult Get(string drive) => null;
    public Task<DiskStatsResult> GetAsync(string drive) => null;
    public void Dispose() { }
}
