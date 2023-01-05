using System;
using System.Runtime.CompilerServices;
using Corax;
using Corax.Mappings;
using Corax.Queries;
using Raven.Server.Documents.Queries;
using Sparrow.Extensions;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryOptimizer;

public readonly struct CoraxBooleanItem : IQueryMatch
{
    public readonly FieldMetadata Field;
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

    public CoraxBooleanItem(IndexSearcher searcher, Index index, FieldMetadata field, object term, UnaryMatchOperation operation, IQueryScoreFunction scoreFunction = default)
    {
        _scoreFunction = scoreFunction;
        Field = field;
        var ticks = default(long);
        
        _isTime = term is not null && index.IndexFieldsPersistence.HasTimeValues(Field.FieldName.ToString()) && QueryBuilderHelper.TryGetTime(index, term, out ticks);
        Term = _isTime ? ticks : term;
        Operation = operation;
        _indexSearcher = searcher;

        Unsafe.SkipInit(out Term2);
        Unsafe.SkipInit(out BetweenLeft);
        Unsafe.SkipInit(out BetweenRight);

        if (operation is UnaryMatchOperation.Equals or UnaryMatchOperation.NotEquals)
        {
            TermAsString = QueryBuilderHelper.CoraxGetValueAsString(term);
            Count = searcher.TermAmount(Field, TermAsString);
        }
        else
        {
            Unsafe.SkipInit(out TermAsString);
            Count = searcher.GetEntriesAmountInField(Field);
        }
    }

    public CoraxBooleanItem(IndexSearcher searcher, Index index, FieldMetadata field, object term1, object term2, UnaryMatchOperation operation, UnaryMatchOperation left, UnaryMatchOperation right, IQueryScoreFunction scoreFunction = default) : this(searcher, index, field, term1, operation, scoreFunction)
    {
        //Between handler
        
        if (_isTime) //found time at `Term1`, lets check if second item also contains time
        {
            if (term2 != null && index.IndexFieldsPersistence.HasTimeValues(field.FieldName.ToString()) && QueryBuilderHelper.TryGetTime(index, term2, out var ticks))
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
            IQueryMatch match = _indexSearcher.TermQuery(Field, TermAsString);
            if (Operation is UnaryMatchOperation.NotEquals)
                match = _indexSearcher.AndNot(_indexSearcher.ExistsQuery(Field), match);
            if (_scoreFunction is NullScoreFunction)
                return match;
            return _indexSearcher.Boost(match, _scoreFunction);
        }

        IQueryMatch baseMatch;

        if (Operation is UnaryMatchOperation.Between)
        {
            baseMatch = (Term, Term2) switch
            {
                (long l, long l2) => _indexSearcher.BetweenQuery(Field, l, l2, _scoreFunction, leftSide: BetweenLeft, rightSide: BetweenRight),
                (double d, double d2) => _indexSearcher.BetweenQuery(Field, d, d2, _scoreFunction, leftSide: BetweenLeft, rightSide: BetweenRight),
                (string s, string s2) => _indexSearcher.BetweenQuery(Field, s, s2, _scoreFunction, leftSide: BetweenLeft, rightSide: BetweenRight),
                (long l, double d) => _indexSearcher.BetweenQuery(Field, Convert.ToDouble(l), d, _scoreFunction, leftSide: BetweenLeft, rightSide: BetweenRight),
                (double d, long l) => _indexSearcher.BetweenQuery(Field, d, Convert.ToDouble(l), _scoreFunction, leftSide: BetweenLeft, rightSide: BetweenRight),
                _ => throw new InvalidOperationException($"UnaryMatchOperation {Operation} is not supported for type {Term.GetType()}")
            };
        }
        else
        {
            baseMatch = (Operation, Term) switch
            {
                (UnaryMatchOperation.LessThan, long term) => _indexSearcher.LessThanQuery(Field, term, _scoreFunction, false),
                (UnaryMatchOperation.LessThan, double term) => _indexSearcher.LessThanQuery(Field, term, _scoreFunction, false),
                (UnaryMatchOperation.LessThan, string term) => _indexSearcher.LessThanQuery(Field, term, _scoreFunction, false),

                (UnaryMatchOperation.LessThanOrEqual, long term) => _indexSearcher.LessThanOrEqualsQuery(Field, term, _scoreFunction, false),
                (UnaryMatchOperation.LessThanOrEqual, double term) => _indexSearcher.LessThanOrEqualsQuery(Field, term, _scoreFunction, false),
                (UnaryMatchOperation.LessThanOrEqual, string term) => _indexSearcher.LessThanOrEqualsQuery(Field, term, _scoreFunction, false),

                (UnaryMatchOperation.GreaterThan, long term) => _indexSearcher.GreaterThanQuery(Field, term, _scoreFunction, false),
                (UnaryMatchOperation.GreaterThan, double term) => _indexSearcher.GreaterThanQuery(Field, term, _scoreFunction, false),
                (UnaryMatchOperation.GreaterThan, string term) => _indexSearcher.GreaterThanQuery(Field, term, _scoreFunction, false),


                (UnaryMatchOperation.GreaterThanOrEqual, long term) => _indexSearcher.GreatThanOrEqualsQuery(Field, term, _scoreFunction, false),
                (UnaryMatchOperation.GreaterThanOrEqual, double term) => _indexSearcher.GreatThanOrEqualsQuery(Field, term, _scoreFunction, false),
                (UnaryMatchOperation.GreaterThanOrEqual, string term) => _indexSearcher.GreatThanOrEqualsQuery(Field, term, _scoreFunction, false),
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
            return $"Field: {Field.ToString()} {Environment.NewLine}" +
                   $"Operation: '{Operation}'{Environment.NewLine}" +
                   $"Between options:{Environment.NewLine}" +
                   $"\tLeft operation: '{BetweenLeft}'{Environment.NewLine}" +
                   $"\tRight operation: '{BetweenRight}'{Environment.NewLine}" +
                   $"Left term: '{Term}'{Environment.NewLine}" +
                   $"Right term: '{Term2}'{Environment.NewLine}";
        }

        return $"Field: {Field.ToString()} {Environment.NewLine}" +
               $"Term: '{Term}'{Environment.NewLine}" +
               $"Operation: '{Operation}'{Environment.NewLine}";
    }
}
