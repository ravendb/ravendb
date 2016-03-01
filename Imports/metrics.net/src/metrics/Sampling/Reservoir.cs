

namespace Metrics.Sampling
{
    public interface Reservoir
    {
        long Count { get; }
        int Size { get; }
        void Update(long value);
        Snapshot GetSnapshot(bool resetReservoir = false);
        void Reset();
    }
}
