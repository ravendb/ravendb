using System.Collections.Generic;
using System.Linq;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Sparrow.Extensions;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryOptimizer;

public sealed class CoraxOrQueries : CoraxBooleanQueryBase
{
    private List<CoraxBooleanItem> _unaryMatchesList;
    private Dictionary<FieldMetadata, List<string>> _termMatchesList; 
    private List<IQueryMatch> _complexMatches;

    public CoraxOrQueries(IndexSearcher indexSearcher, CoraxQueryBuilder.Parameters parameters) : base(indexSearcher, parameters)
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
                if (EqualsScoreFunctions(mao) == false)
                    return false; 
                mao.Boosting = null;
                itemToAdd = mao.Materialize();
                _hasBinary |= mao.HasBinary;
                break;
            case BoostingMatch boostingMatch:
                if (Boosting.HasValue == false || 
                    Boosting.Value.AlmostEquals(boostingMatch.BoostFactor) == false)
                    return false;
                break;
        }

        (_complexMatches ??= new List<IQueryMatch>()).Add(itemToAdd);
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
        else if (itemToAdd.Operation is UnaryMatchOperation.Equals && itemToAdd.TermAsString != null)
        {
            _termMatchesList ??= new();

            if (_termMatchesList.TryGetValue(itemToAdd.Field, out var list) == false)
                _termMatchesList.Add(itemToAdd.Field, new List<string>() {itemToAdd.TermAsString});
            else
                list.Add(itemToAdd.TermAsString);
        }
        else
        {
            _complexMatches ??= new();
            CoraxQueryBuilder.StreamingOptimization disableOptimization = default;
            _complexMatches.Add(itemToAdd.Materialize(ref disableOptimization));
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
            {
                if (terms.Count == 1)
                {
                   AddToQueryTree(IndexSearcher.TermQuery(field, terms[0])); 
                }
                else
                {
                    
                    AddToQueryTree(IndexSearcher.InQuery(field, terms));
                }
            }
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
