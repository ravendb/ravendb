using Corax.Utils;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public readonly struct CoraxIndexingStats(IndexingStatsScope indexingStatsScope) : ICoraxStatsScope
{
    private readonly IndexingStatsScope _stats = indexingStatsScope;
    
    public ICoraxStatsScope For(string name, bool start = true)
    {
        return new CoraxIndexingStats(_stats.For(name, start));
    }
    
    public void Dispose()
    {
        _stats?.Dispose();
    }
}
