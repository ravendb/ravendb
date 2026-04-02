using System;
using Corax.Querying.Matches.Meta;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryOptimizer;

public struct CoraxWhenQuery : IQueryMatch
{
    private const string ExceptionMessage = $"{nameof(CoraxWhenQuery)} is used for type relaxation and is not a valid struct.";
    
    public long Count => throw new NotSupportedException(ExceptionMessage);
    public SkipSortingResult AttemptToSkipSorting() => throw new NotSupportedException(ExceptionMessage);

    public QueryCountConfidence Confidence => throw new NotSupportedException(ExceptionMessage);
    public bool IsBoosting => throw new NotSupportedException(ExceptionMessage);
    public int Fill(Span<long> matches) => throw new NotSupportedException(ExceptionMessage);

    public int AndWith(Span<long> buffer, int matches) => throw new NotSupportedException(ExceptionMessage);

    public void Score(Span<long> matches, Span<float> scores, float boostFactor) => throw new NotSupportedException(ExceptionMessage);

    public QueryInspectionNode Inspect() => throw new NotSupportedException(ExceptionMessage);

    public DuplicatesOccurrence DuplicatesOccurrenceStatus => throw new NotSupportedException(ExceptionMessage);
}
