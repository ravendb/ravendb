using System;

namespace Corax.Utils;

public interface ICoraxStatsScope : IDisposable
{
    ICoraxStatsScope For(string name, bool start = true);
}

public static class CommitOperation
{
    public const string TextualValues = nameof(TextualValues);
    public const string FloatingValues = nameof(FloatingValues);
    public const string IntegerValues = nameof(IntegerValues);
    public const string Deletions = nameof(Deletions);
    public const string Suggestions = nameof(Suggestions);
    public const string SpatialValues = nameof(SpatialValues);
    public const string StoredValues = nameof(StoredValues);
}

internal struct EmptyStatsScope : ICoraxStatsScope
{
    public ICoraxStatsScope For(string name, bool start = true)
    {
        return default;
    }
    
    public void Dispose()
    {
    }
}
