using System.Collections.Generic;
using System.Linq;
using Corax;
using Corax.Queries;
using Sparrow.Extensions;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryOptimizer;

public class CoraxOrQueries : CoraxBooleanQueryBase
{
    private List<CoraxBooleanItem> _unaryMatchesList;
    private Dictionary<(string FieldName, int FieldId), List<string>> _termMatchesList; 
    private List<IQueryMatch> _complexMatches;

    public CoraxOrQueries(IndexSearcher indexSearcher, IQueryScoreFunction scoreFunction) : base(indexSearcher, scoreFunction)
    {
    }

    private bool TryMerge(CoraxOrQueries other)
    {
        bool canMerge = (other.ScoreFunction, ScoreFunction) switch
        {
            (NullScoreFunction, NullScoreFunction) => true,
            (ConstantScoreFunction l, ConstantScoreFunction r) => l.Value.AlmostEquals(r.Value),
            (_, _) => false
        };

        if (canMerge == false)
            return false;

        _hasBinary |= other.HasBinary;

        if (other._termMatchesList != null)
        {
            _termMatchesList ??= new();
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
                itemToAdd = mao.Materialize();
                _hasBinary |= mao.HasBinary;
                break;
        }

        (_complexMatches ??= new()).Add(itemToAdd);
        return true;
    }

    private bool TryAddItem(CoraxBooleanItem itemToAdd)
    {
        if (itemToAdd.CompareScoreFunction(ScoreFunction) == false)
            return false;

        if (itemToAdd.Operation is not UnaryMatchOperation.Equals)
        {
            _unaryMatchesList ??= new();
            _unaryMatchesList.Add(itemToAdd);
        }
        else
        {
            _termMatchesList ??= new();

            if (_termMatchesList.TryGetValue((itemToAdd.Name, itemToAdd.FieldId), out var list) == false)
                _termMatchesList.Add((itemToAdd.Name, itemToAdd.FieldId), new List<string>() {itemToAdd.TermAsString});
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
            foreach (var ((fieldName, fieldId), terms) in _termMatchesList)
                AddToQueryTree(IndexSearcher.InQuery(fieldName, terms, fieldId));
        }

        if (ScoreFunction is not NullScoreFunction && ScoreFunction != null)
            baseQuery = IndexSearcher.Boost(baseQuery, ScoreFunction);
        
        if (_complexMatches != null)
        {
            foreach (var complex in _complexMatches ?? Enumerable.Empty<IQueryMatch>())
                AddToQueryTree(complex);
        }

        return baseQuery;
        
        void AddToQueryTree(IQueryMatch query)
        {
            baseQuery = baseQuery is null
                ? query
                : IndexSearcher.Or(baseQuery, query);
        }
    }
}
