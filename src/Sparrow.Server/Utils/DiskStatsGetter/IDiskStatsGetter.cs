using System.Threading.Tasks;

namespace Sparrow.Server.Utils.DiskStatsGetter;

public interface IDiskStatsGetter
{
    DiskStatsResult Get(string drive);
    Task<DiskStatsResult> GetAsync(string drive);
}
