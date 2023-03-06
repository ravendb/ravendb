using System.Collections.Generic;
using System.Linq;
using Corax;
using Corax.Mappings;
using Corax.Queries;
using Sparrow.Extensions;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryOptimizer;

public class CoraxOrQueries : CoraxBooleanQueryBase
{
    private List<CoraxBooleanItem> _unaryMatchesList;
    private Dictionary<FieldMetadata, List<string>> _termMatchesList; 
    private List<IQueryMatch> _complexMatches;

    public CoraxOrQueries(IndexSearcher indexSearcher) : base(indexSearcher)
    {
    }

    private bool TryMerge(CoraxOrQueries other)
    {
        bool canMerge = EqualsScoreFunctions(other);

        if (canMerge == false)
            return false;

        _hasBinary |= other.HasBinary;

        if (other._termMatchesList != null)
        {
            _termMatchesList ??= new(FieldMetadataComparer.Instance);
            foreach (var (key, value) in other._termMatchesList)
            {
                if (_termMatchesList.TryGetValue(key, out var list))
                    list.AddRange(value);
                else
                    _termMatchesList.Add(key, value);
            }
        }
        
        if (other._complexMatches != null)
        {
            _complexMatches ??= new();
            _complexMatches.AddRange(other._complexMatches);
        }
        
        if (other._unaryMatchesList != null)
        {
            _unaryMatchesList ??= new();
            _unaryMatchesList.AddRange(other._unaryMatchesList);
        }

        return true;
    }

    public bool TryAddItem(IQueryMatch itemToAdd)
    {
        switch (itemToAdd)
        {
            case CoraxOrQueries moq:
                return TryMerge(moq);
            case CoraxBooleanItem cbi:
                return TryAddItem(cbi);
            case CoraxAndQueries mao:
                // We popup inner boosting to this
                if (EqualsScoreFunctions(mao))
                    mao.Boosting = null;
                else
                    return false;
                
                itemToAdd = mao.Materialize();
                _hasBinary |= mao.HasBinary;
                break;
            case BoostingMatch boostingMatch:
                if (Boosting.HasValue && Boosting.Value.AlmostEquals(boostingMatch.BoostFactor))
                    (_complexMatches ??= new()).Add(itemToAdd);
                else
                    return false;
                break;
                
            default:
                    (_complexMatches ??= new()).Add(itemToAdd);
                break;
        }
        
        return true;
    }

    private bool TryAddItem(CoraxBooleanItem itemToAdd)
    {
        if (EqualsScoreFunctions(itemToAdd) == false)
            return false;
        
        if (itemToAdd.Operation is not UnaryMatchOperation.Equals)
        {
            _unaryMatchesList ??= new();
            _unaryMatchesList.Add(itemToAdd);
        }
        else
        {
            _termMatchesList ??= new();

            if (_termMatchesList.TryGetValue(itemToAdd.Field, out var list) == false)
                _termMatchesList.Add(itemToAdd.Field, new List<string>() {itemToAdd.TermAsString});
            else
                list.Add(itemToAdd.TermAsString);
        }

        return true;
    }

    public override IQueryMatch Materialize()
    {
        IQueryMatch baseQuery = null;
        
        if (_unaryMatchesList != null)
        {
            foreach (var unaryMatch in _unaryMatchesList)
            {
                var nextQuery = TransformCoraxBooleanItemIntoQueryMatch(unaryMatch);
                AddToQueryTree(nextQuery);
            }
        }
        
        if (_termMatchesList != null)
        {
            foreach (var (field, terms) in _termMatchesList)
                AddToQueryTree(IndexSearcher.InQuery(field, terms));
        }
        
        if (_complexMatches != null)
        {
            foreach (var complex in _complexMatches ?? Enumerable.Empty<IQueryMatch>())
                AddToQueryTree(complex);
        }

        if (Boosting.HasValue)
            baseQuery = IndexSearcher.Boost(baseQuery, Boosting.Value);
        
        return baseQuery;
        
        void AddToQueryTree(IQueryMatch query)
        {
            baseQuery = baseQuery is null
                ? query
                : IndexSearcher.Or(baseQuery, query);
        }
    }
}
