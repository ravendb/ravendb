using System;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Utils;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryOptimizer;

public sealed class CoraxVectorItem(CoraxQueryBuilder.Parameters parameters) : IQueryMatch, ICoraxClause
{
    private bool _isEmpty;
    private FieldMetadata _field;
    private int _numberOfCandidates;
    private float _minimumDistance;
    private bool _isExact;
    private string _documentId;
    private VectorValue _vectorToSearch;
    private VectorValue[] _vectorsToSearch;
    private readonly bool _isVectorSingleClause = parameters.IsVectorSingleClause;
    public float? Boosting { get; set; }

    public static CoraxVectorItem BuildForDocVector(CoraxQueryBuilder.Parameters parameters, FieldMetadata field, string documentId, in int numberOfCandidates, in float minimumDistance, in bool isExact)
    {
        return new(parameters)
        {
            _field = field,
            _isExact = isExact,
            _numberOfCandidates = numberOfCandidates,
            _minimumDistance = minimumDistance,
            _documentId = documentId
        };
    }

    public static CoraxVectorItem BuildEmpty(CoraxQueryBuilder.Parameters parameters)
    {
        return new CoraxVectorItem(parameters){_isEmpty = true};
    }
    
    public static CoraxVectorItem BuildSingleVector(CoraxQueryBuilder.Parameters parameters, FieldMetadata field, VectorValue vectorToSearch, in int numberOfCandidates, in float minimumDistance, in bool isExact)
    {
        return new(parameters)
        {
            _field = field,
            _isExact = isExact,
            _numberOfCandidates = numberOfCandidates,
            _minimumDistance = minimumDistance,
            _vectorToSearch = vectorToSearch,
        };
    }

    public static CoraxVectorItem BuildMultiVector(CoraxQueryBuilder.Parameters parameters, FieldMetadata field, VectorValue[] vectorToSearch, in int numberOfCandidates, in float minimumDistance, in bool isExact)
    {
        return new(parameters)
        {
            _field = field,
            _isExact = isExact,
            _numberOfCandidates = numberOfCandidates,
            _minimumDistance = minimumDistance,
            _vectorsToSearch = vectorToSearch
        };
    }

    public IQueryMatch Materialize(IQueryMatch inner)
    {
        if (_isEmpty)
            return parameters.IndexSearcher.EmptyMatch();
        
        IQueryMatch vs;
        if (_documentId != null)
        {
            vs = parameters.IndexSearcher.VectorSearch(_field, _documentId, _minimumDistance, _numberOfCandidates, _isExact, parameters.IsVectorSingleClause, inner, parameters.Index.Configuration.CoraxVectorSearchScanningThreshold);
        }
        else if (_vectorsToSearch is not null)
        {
            vs = parameters.IndexSearcher.MultiVectorSearch(_field, _vectorsToSearch, _minimumDistance, _numberOfCandidates, _isExact, _isVectorSingleClause, inner, parameters.Index.Configuration.CoraxVectorSearchScanningThreshold);
        }
        else
        {
            vs = parameters.IndexSearcher.VectorSearch(_field, _vectorToSearch, _minimumDistance, _numberOfCandidates, _isExact, _isVectorSingleClause, inner, parameters.Index.Configuration.CoraxVectorSearchScanningThreshold);
        }

        return Boosting is null ? vs : parameters.IndexSearcher.Boost(vs, Boosting.Value);
    }

    #region IQueryMatch methods

    public long Count => throw new NotSupportedException(IQueryMatchUsageException);
    public SkipSortingResult AttemptToSkipSorting() => throw new NotSupportedException(IQueryMatchUsageException);

    public QueryCountConfidence Confidence { get => throw new NotSupportedException(IQueryMatchUsageException); }
    public bool IsBoosting { get; }
    public int Fill(Span<long> matches) => throw new NotSupportedException(IQueryMatchUsageException);

    public int AndWith(Span<long> buffer, int matches) => throw new NotSupportedException(IQueryMatchUsageException);

    public void Score(Span<long> matches, Span<float> scores, float boostFactor) => throw new NotSupportedException(IQueryMatchUsageException);

    public QueryInspectionNode Inspect() => throw new NotSupportedException(IQueryMatchUsageException);

    public DuplicatesOccurrence DuplicatesOccurrenceStatus { get => throw new NotSupportedException(IQueryMatchUsageException); }


    private const string IQueryMatchUsageException = $"You tried to use {nameof(CoraxVectorItem)} as normal querying function. This class is only for type - relaxation inside {nameof(CoraxQueryBuilder)} to build query.";

    #endregion
}
