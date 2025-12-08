using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryOptimizer;

public sealed class CoraxOrQueries : CoraxBooleanQueryBase
{
    private Dictionary<FieldMetadata, List<string>> _termMatchesList;

    private CoraxOrQueries(CoraxQueryBuilder.Parameters parameters) : base(parameters)
    {
    }

    public static CoraxOrQueries Or(CoraxQueryBuilder.Parameters parameters, IQueryMatch left, IQueryMatch right)
    {
        {
            if (left is CoraxOrQueries leftOr && right is CoraxOrQueries rightOr)
                return MergeOrCreateTwoOrQueries(parameters, leftOr, rightOr);
        }

        {
            // It does not have boosting, so we can merge it.
            if (left is CoraxOrQueries { HasBoosting: false } leftOr)
                return (CoraxOrQueries)leftOr.Add(right);

            if (right is CoraxOrQueries { HasBoosting: false } rightOr)
                return (CoraxOrQueries)rightOr.Add(left);
        }
        
        return CreateNew(parameters, left, right);
    }
    
    private static CoraxOrQueries CreateNew(CoraxQueryBuilder.Parameters parameters, IQueryMatch left, IQueryMatch right)
    {
        var orClause = new CoraxOrQueries(parameters);
        orClause.Add(left);
        orClause.Add(right);
        return orClause;
    }
    
    private static CoraxOrQueries MergeOrCreateTwoOrQueries(CoraxQueryBuilder.Parameters parameters, CoraxOrQueries left, CoraxOrQueries right)
    {
        if (left.EqualsScoreFunctions(right))
        {
            left.AddOrQueries(right);
            return left;
        }

        return CreateNew(parameters, left, right);
    }

    private void AddOrQueries(CoraxOrQueries other)
    {
        _hasBinary |= other.HasBinary;

        if (other._termMatchesList != null)
        {
            _termMatchesList ??= new(FieldMetadataComparer.Instance);
            foreach (var (key, value) in other._termMatchesList)
            {
                ref var list = ref CollectionsMarshal.GetValueRefOrNullRef(_termMatchesList, key);
                if (Unsafe.IsNullRef(ref list))
                    _termMatchesList.Add(key, value);
                else
                    list.AddRange(value);
            }
        }

        if (other.ComplexMatches != null)
        {
            if (ComplexMatches == null)
                ComplexMatches = other.ComplexMatches;
            else
                ComplexMatches.AddRange(other.ComplexMatches);
        }

        if (other.QueryStack != null)
        {
            if (QueryStack == null)
                QueryStack = other.QueryStack;
            else
                QueryStack.AddRange(other.QueryStack);
        }

        if (other.VectorStack != null)
        {
            if (VectorStack == null)
                VectorStack = other.VectorStack;
            else 
                VectorStack.AddRange(other.VectorStack);
        }
    }
    
    protected override void AddCoraxBooleanItem(CoraxBooleanItem itemToAdd)
    {
        if (itemToAdd.Boosting.HasValue == false && itemToAdd.Operation is not UnaryMatchOperation.Equals)
        {
            QueryStack ??= new();
            QueryStack.Add(itemToAdd);
        }
        else if (itemToAdd.Boosting.HasValue == false && itemToAdd.Operation is UnaryMatchOperation.Equals && itemToAdd.TermAsString != null)
        {
            _termMatchesList ??= new();

            if (_termMatchesList.TryGetValue(itemToAdd.Field, out var list) == false)
                _termMatchesList.Add(itemToAdd.Field, new List<string>() { itemToAdd.TermAsString });
            else
                list.Add(itemToAdd.TermAsString);
        }
        else
        {
            QueryStack ??= new();
            QueryStack.Add(itemToAdd);
        }
    }

    public override IQueryMatch Materialize()
    {
        IQueryMatch baseQuery = null;

        if (QueryStack != null)
        {
            foreach (var unaryMatch in QueryStack)
            {
                var nextQuery = unaryMatch.Materialize(ref _parameters.StreamingDisabled);
                AddToQueryTree(nextQuery);
            }
            QueryStack = null;
        }

        foreach (var (field, terms) in _termMatchesList ?? [])
        {
            if (terms.Count == 1)
            {
                AddToQueryTree(_parameters.IndexSearcher.TermQuery(field, terms[0]));
            }
            else
            {
                AddToQueryTree(_parameters.IndexSearcher.InQuery(field, terms));
            }
        }
        _termMatchesList = null;

        foreach (var complex in ComplexMatches ?? Enumerable.Empty<IQueryMatch>())
            AddToQueryTree(complex);
        ComplexMatches = null;
        
        foreach (var vector in VectorStack ?? Enumerable.Empty<CoraxVectorItem>())
        {
            AddToQueryTree(vector.Materialize(null));
        }
        
        VectorStack = null;

        if (Boosting.HasValue)
            baseQuery = _parameters.IndexSearcher.Boost(baseQuery, Boosting.Value);

        return baseQuery;

        void AddToQueryTree(IQueryMatch query)
        {
            baseQuery = baseQuery is null
                ? query
                : _parameters.IndexSearcher.Or(baseQuery, query);
        }
    }
}
