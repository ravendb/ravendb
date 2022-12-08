using System;
using System.Runtime.CompilerServices;
using Corax;
using Corax.Queries;
using Raven.Server.Documents.Queries;
using Sparrow.Extensions;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryOptimizer;

public readonly struct CoraxBooleanItem : IQueryMatch
{
    public readonly string Name;
    public readonly int FieldId;
    public readonly object Term;
    public readonly object Term2;
    public readonly string TermAsString;
    public readonly UnaryMatchOperation Operation;
    public readonly UnaryMatchOperation BetweenLeft;
    public readonly UnaryMatchOperation BetweenRight;
    private readonly IQueryScoreFunction _scoreFunction;
    private readonly IndexSearcher _indexSearcher;
    private readonly bool _isTime; 
    public bool IsBoosting => _scoreFunction is not NullScoreFunction;

    public long Count { get; }

    public CoraxBooleanItem(IndexSearcher searcher, Index index, string name, int fieldId, object term, UnaryMatchOperation operation, IQueryScoreFunction scoreFunction = default)
    {
        _scoreFunction = scoreFunction;
        Name = name;
        FieldId = fieldId;
        var ticks = default(long);
        _isTime = term is not null && index.IndexFieldsPersistence.HasTimeValues(name) && QueryBuilderHelper.TryGetTime(index, term, out ticks);
        Term = _isTime ? ticks : term;

        Operation = operation;
        _indexSearcher = searcher;

        Unsafe.SkipInit(out Term2);
        Unsafe.SkipInit(out BetweenLeft);
        Unsafe.SkipInit(out BetweenRight);

        if (operation is UnaryMatchOperation.Equals or UnaryMatchOperation.NotEquals)
        {
            TermAsString = QueryBuilderHelper.CoraxGetValueAsString(term);
            Count = searcher.TermAmount(name, TermAsString, fieldId);
        }
        else
        {
            Unsafe.SkipInit(out TermAsString);
            Count = searcher.GetEntriesAmountInField(name);
        }
    }

    public CoraxBooleanItem(IndexSearcher searcher, Index index, string name, int fieldId, object term1, object term2, UnaryMatchOperation operation, UnaryMatchOperation left, UnaryMatchOperation right, IQueryScoreFunction scoreFunction = default) : this(searcher, index, name, fieldId, term1, operation, scoreFunction)
    {
        //Between handler
        
        if (_isTime) //found time at `Term1`, lets check if second item also contains time
        {
            if (term2 != null && index.IndexFieldsPersistence.HasTimeValues(name) && QueryBuilderHelper.TryGetTime(index, term2, out var ticks))
            {
                Term2 = ticks;
            }
            else  //not found, lets revert Term1 
            {
                Term = term1;
                Term2 = term2;
                _isTime = false;
            }
        }
        else
        {
            Term2 = term2;
        }

        BetweenRight = right;
        BetweenLeft = left;
    }
    
    public static bool CanBeMergedForAnd(CoraxBooleanItem lhs, CoraxBooleanItem rhs)
    {
        return (lhs._scoreFunction, rhs._scoreFunction) switch
        {
            (NullScoreFunction, NullScoreFunction) => true,
            (ConstantScoreFunction lcsf, ConstantScoreFunction rcsf) => lcsf.Value.AlmostEquals(rcsf.Value),
            _ => false
        };
    }

    public static bool CanBeMergedForOr(CoraxBooleanItem lhs, CoraxBooleanItem rhs)
    {
        return CanBeMergedForAnd(lhs, rhs) && lhs.Name == rhs.Name && lhs.FieldId == rhs.FieldId;
    }

    public bool CompareScoreFunction(IQueryScoreFunction scoreFunction)
    {
        return (scoreFunction, _scoreFunction) switch
        {
            (NullScoreFunction, NullScoreFunction) => true,
            (ConstantScoreFunction lcsf, ConstantScoreFunction rcsf) => lcsf.Value.AlmostEquals(rcsf.Value),
            _ => false
        };
    }
    
    public IQueryMatch Materialize()
    {
        if (Operation is UnaryMatchOperation.Equals or UnaryMatchOperation.NotEquals)
        {
            IQueryMatch match = _indexSearcher.TermQuery(Name, TermAsString, FieldId);
            if (Operation is UnaryMatchOperation.NotEquals)
                match = _indexSearcher.AndNot(_indexSearcher.ExistsQuery(Name), match);
            if (_scoreFunction is NullScoreFunction)
                return match;
            return _indexSearcher.Boost(match, _scoreFunction);
        }

        IQueryMatch baseMatch;

        if (Operation is UnaryMatchOperation.Between)
        {
            baseMatch = (Term, Term2) switch
            {
                (long l, long l2) => _indexSearcher.BetweenQuery(Name, l, l2, _scoreFunction, leftSide: BetweenLeft, rightSide: BetweenRight, fieldId: FieldId),
                (double d, double d2) => _indexSearcher.BetweenQuery(Name, d, d2, _scoreFunction, leftSide: BetweenLeft, rightSide: BetweenRight, fieldId: FieldId),
                (string s, string s2) => _indexSearcher.BetweenQuery(Name, s, s2, _scoreFunction, leftSide: BetweenLeft, rightSide: BetweenRight, fieldId: FieldId),
                (long l, double d) => _indexSearcher.BetweenQuery(Name, Convert.ToDouble(l), d, _scoreFunction, leftSide: BetweenLeft, rightSide: BetweenRight, fieldId: FieldId),
                (double d, long l) => _indexSearcher.BetweenQuery(Name, d, Convert.ToDouble(l), _scoreFunction, leftSide: BetweenLeft, rightSide: BetweenRight, fieldId: FieldId),
                _ => throw new InvalidOperationException($"UnaryMatchOperation {Operation} is not supported for type {Term.GetType()}")
            };
        }
        else
        {
            baseMatch = (Operation, Term) switch
            {
                (UnaryMatchOperation.LessThan, long term) => _indexSearcher.LessThanQuery(Name, term, _scoreFunction, false, FieldId),
                (UnaryMatchOperation.LessThan, double term) => _indexSearcher.LessThanQuery(Name, term, _scoreFunction, false, FieldId),
                (UnaryMatchOperation.LessThan, string term) => _indexSearcher.LessThanQuery(Name, term, _scoreFunction, false, FieldId),

                (UnaryMatchOperation.LessThanOrEqual, long term) => _indexSearcher.LessThanOrEqualsQuery(Name, term, _scoreFunction, false, FieldId),
                (UnaryMatchOperation.LessThanOrEqual, double term) => _indexSearcher.LessThanOrEqualsQuery(Name, term, _scoreFunction, false, FieldId),
                (UnaryMatchOperation.LessThanOrEqual, string term) => _indexSearcher.LessThanOrEqualsQuery(Name, term, _scoreFunction, false, FieldId),

                (UnaryMatchOperation.GreaterThan, long term) => _indexSearcher.GreaterThanQuery(Name, term, _scoreFunction, false, FieldId),
                (UnaryMatchOperation.GreaterThan, double term) => _indexSearcher.GreaterThanQuery(Name, term, _scoreFunction, false, FieldId),
                (UnaryMatchOperation.GreaterThan, string term) => _indexSearcher.GreaterThanQuery(Name, term, _scoreFunction, false, FieldId),


                (UnaryMatchOperation.GreaterThanOrEqual, long term) => _indexSearcher.GreatThanOrEqualsQuery(Name, term, _scoreFunction, false, FieldId),
                (UnaryMatchOperation.GreaterThanOrEqual, double term) => _indexSearcher.GreatThanOrEqualsQuery(Name, term, _scoreFunction, false, FieldId),
                (UnaryMatchOperation.GreaterThanOrEqual, string term) => _indexSearcher.GreatThanOrEqualsQuery(Name, term, _scoreFunction, false, FieldId),
                _ => throw new ArgumentException("This is only Greater*/Less* Query part")
            };
        }

        return baseMatch;
    }
    
    public QueryCountConfidence Confidence => throw new InvalidOperationException(IQueryMatchUsageException);
    public int Fill(Span<long> matches) => throw new InvalidOperationException(IQueryMatchUsageException);

    public int AndWith(Span<long> buffer, int matches) => throw new InvalidOperationException(IQueryMatchUsageException);

    public void Score(Span<long> matches, Span<float> scores) => throw new InvalidOperationException(IQueryMatchUsageException);

    public QueryInspectionNode Inspect() => throw new InvalidOperationException(IQueryMatchUsageException);
    private const string IQueryMatchUsageException = $"You tried to use {nameof(CoraxAndQueries)} as normal querying function. This class is only for type - relaxation inside {nameof(CoraxQueryBuilder)} to build big UnaryMatch stack";

    public override string ToString()
    {
        if (Operation is UnaryMatchOperation.Between or UnaryMatchOperation.NotBetween)
        {
            return $"Field name: '{Name}'{Environment.NewLine}" +
                   $"Field id: '{FieldId}'{Environment.NewLine}" +
                   $"Operation: '{Operation}'{Environment.NewLine}" +
                   $"Between options:{Environment.NewLine}" +
                   $"\tLeft operation: '{BetweenLeft}'{Environment.NewLine}" +
                   $"\tRight operation: '{BetweenRight}'{Environment.NewLine}" +
                   $"Left term: '{Term}'{Environment.NewLine}" +
                   $"Right term: '{Term2}'{Environment.NewLine}";
        }

        return $"Field name: '{Name}'{Environment.NewLine}" +
               $"Field id: '{FieldId}'{Environment.NewLine}" +
               $"Term: '{Term}'{Environment.NewLine}" +
               $"Operation: '{Operation}'{Environment.NewLine}";
    }
}
