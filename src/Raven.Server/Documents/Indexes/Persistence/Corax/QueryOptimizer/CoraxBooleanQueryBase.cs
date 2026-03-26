using System;
using System.Collections.Generic;
using Corax.Querying.Matches.Meta;
using Sparrow.Extensions;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryOptimizer;

public abstract class CoraxBooleanQueryBase(CoraxQueryBuilder.Parameters parameters) : IQueryMatch, ICoraxClause
{
    public bool HasBoosting => Boosting.HasValue;

    protected List<IQueryMatch> ComplexMatches;
    protected List<CoraxBooleanItem> QueryStack;
    protected List<CoraxVectorItem> VectorStack;

    protected CoraxBooleanQueryBase Add(IQueryMatch item)
    {
        switch (item)
        {
            case CoraxOrQueries moq:
                _parameters.BuildSteps?.Add($"Adding CoraxOrQueries to query.");
                AddComplexMatch(moq.Materialize());
                break;
            case CoraxAndQueries caq:
                _parameters.BuildSteps?.Add($"Adding CoraxAndQueries to query.");
                AddComplexMatch(caq.Materialize());
                break;
            case CoraxVectorItem cvi:
                AddCoraxVectorItem(cvi);
                break;
            case CoraxBooleanItem cbi:
                AddCoraxBooleanItem(cbi);
                break;
            default:
                AddComplexMatch(item);
                break;
        }

        return this;
    }

    private void AddCoraxVectorItem(CoraxVectorItem item)
    {
        _parameters.BuildSteps?.Add($"Adding CoraxVectorItem to query.");
        VectorStack ??= new();
        VectorStack.Add(item);
    }

    private void AddComplexMatch(IQueryMatch item)
    {
        _parameters.BuildSteps?.Add($"Adding {item.GetType().Name} to query.");
        ComplexMatches ??= new();
        ComplexMatches.Add(item);
    }

    protected abstract void AddCoraxBooleanItem(CoraxBooleanItem item);


    public abstract IQueryMatch Materialize();
    public float? Boosting { get; set; }
    protected readonly CoraxQueryBuilder.Parameters _parameters = parameters;
    protected bool _hasBinary;
    public bool HasBinary => _hasBinary;


    protected bool EqualsScoreFunctions(CoraxBooleanQueryBase other)
    {
        if (Boosting is null && other.Boosting is null) return true;
        if (Boosting is null || other.Boosting is null) return false;

        return Boosting.Value.AlmostEquals(other.Boosting.Value);
    }

    protected bool EqualsScoreFunctions(CoraxVectorItem other)
    {
        if (Boosting is null && other.Boosting is null) return true;
        if (Boosting is null || other.Boosting is null) return false;

        return Boosting.Value.AlmostEquals(other.Boosting.Value);
    }

    protected bool EqualsScoreFunctions(CoraxBooleanItem other)
    {
        if (Boosting is null && other.Boosting is null) return true;
        if (Boosting is null || other.Boosting is null) return false;

        return Boosting.Value.AlmostEquals(other.Boosting.Value);
    }

    #region IQueryMatchRelaxation

    public DuplicatesOccurrence DuplicatesOccurrenceStatus => throw new InvalidOperationException($"{nameof(DuplicatesOccurrenceStatus)} should never be used in {nameof(CoraxBooleanQueryBase)}");

    public bool IsBoosting => false;

    protected const string QueryMatchUsageException =
        $"You tried to use {nameof(CoraxBooleanQueryBase)} as normal querying function. This class is only for type - relaxation inside {nameof(CoraxQueryBuilder)} to build big UnaryMatch stack";

    public SkipSortingResult AttemptToSkipSorting() => throw new InvalidOperationException(QueryMatchUsageException);

    public long Count => throw new InvalidOperationException(QueryMatchUsageException);
    public QueryCountConfidence Confidence => throw new InvalidOperationException(QueryMatchUsageException);
    public int Fill(Span<long> matches) => throw new InvalidOperationException(QueryMatchUsageException);
    public int AndWith(Span<long> buffer, int matches) => throw new InvalidOperationException(QueryMatchUsageException);
    public void Score(Span<long> matches, Span<float> scores, float boostFactor) => throw new InvalidOperationException(QueryMatchUsageException);
    public QueryInspectionNode Inspect() => throw new InvalidOperationException(QueryMatchUsageException);

    #endregion

}
