using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Raven.Server.Documents.Queries;
using Sparrow.Binary;
using Sparrow.Extensions;
using Voron;
using Constants = Raven.Client.Constants;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryOptimizer;

public struct CoraxBooleanItem : IQueryMatch, ICoraxClause
{
    public readonly FieldMetadata Field;
    public readonly object Term;
    public readonly object Term2;
    public readonly string TermAsString;
    public readonly UnaryMatchOperation Operation;
    public readonly UnaryMatchOperation BetweenLeft;
    public readonly UnaryMatchOperation BetweenRight;
    private readonly IndexSearcher _indexSearcher;
    public bool IsBoosting => Boosting.HasValue;
    public long Count { get; }

    /// <summary>
    /// Indicates if this is a NotEquals operation that can be optimized when combined with AND.
    /// When true, the caller can use MaterializeNegatedTermMatch to get just the term match
    /// and combine it with AndNot instead of And(x, AndNot(AllEntries, term)).
    /// </summary>
    public bool IsNegated => Operation is UnaryMatchOperation.NotEquals;
    
    public DuplicatesOccurrence DuplicatesOccurrenceStatus => throw new InvalidOperationException($"{nameof(DuplicatesOccurrenceStatus)} should never be used in {nameof(CoraxBooleanItem)}");


    private CoraxBooleanItem(IndexSearcher indexSearcher, FieldMetadata field, object term, UnaryMatchOperation operation)
    {
        Field = field;
        Term = term;

        // in case of query "Field != null" or `Field != ""`
        if (Term is null || Term is string s)
            Term = QueryBuilderHelper.CoraxGetValueAsString(Term);


        Operation = operation;
        _indexSearcher = indexSearcher;

        Unsafe.SkipInit(out Term2);
        Unsafe.SkipInit(out BetweenLeft);
        Unsafe.SkipInit(out BetweenRight);

        if (operation is UnaryMatchOperation.Equals or UnaryMatchOperation.NotEquals)
        {
            if (term is not (long or double))
                TermAsString = QueryBuilderHelper.CoraxGetValueAsString(term);

            Count = Term switch
            {
                long l => indexSearcher.NumberOfDocumentsUnderSpecificTerm(Field, l),
                double d => indexSearcher.NumberOfDocumentsUnderSpecificTerm(Field, d),
                _ => indexSearcher.NumberOfDocumentsUnderSpecificTerm(Field, TermAsString)
            };
        }
        else
        {
            Unsafe.SkipInit(out TermAsString);
            Count = indexSearcher.GetTermAmountInField(Field);
        }
    }


    private CoraxBooleanItem(IndexSearcher indexSearcher, FieldMetadata field, object leftTerm, object rightTerm,
        UnaryMatchOperation leftOperation, UnaryMatchOperation rightOperation)
    {
        Operation = UnaryMatchOperation.Between;
        BetweenLeft = leftOperation;
        BetweenRight = rightOperation;
        Field = field;
        Count = indexSearcher.GetTermAmountInField(Field);
        _indexSearcher = indexSearcher;
        Term = leftTerm is not string ? leftTerm : QueryBuilderHelper.CoraxGetValueAsString(leftTerm);
        Term2 = rightTerm is not string ? rightTerm : QueryBuilderHelper.CoraxGetValueAsString(rightTerm);
    }
    
    public static IQueryMatch Build(IndexSearcher indexSearcher, Index index, FieldMetadata field, object term, UnaryMatchOperation operation, ref CoraxQueryBuilder.StreamingOptimization streamingOptimization)
    {
        long timeTicks = 0L;
        var fieldHasTime = index.IndexFieldsPersistence.HasTimeValues(field.FieldName.ToString());
        var isTimeValue = fieldHasTime
                          && term is not null
                          && QueryBuilderHelper.TryGetTime(index, term, out timeTicks);
        term = isTimeValue ? timeTicks : term;
        
        return new CoraxBooleanItem(indexSearcher, field, term, operation);
    }

    public static IQueryMatch BuildBetween(IndexSearcher indexSearcher, Index index, FieldMetadata field, object leftValue, object rightValue,
        UnaryMatchOperation leftOperator, UnaryMatchOperation rightOperator, ref CoraxQueryBuilder.StreamingOptimization streamingOptimization)
    {
        var leftIsUnbounded = leftValue is null or Constants.Documents.Querying.Terms.LeftNullValueOfBetweenQuery;
        var rightIsUnbounded = rightValue is null or Constants.Documents.Querying.Terms.RightNullValueOfBetweenQuery;

        switch (IsLeftUnbounded: leftIsUnbounded, IsRightUnbounded: rightIsUnbounded)
        {
            case (IsLeftUnbounded: true, IsRightUnbounded: true):
            {
                Debug.Assert(streamingOptimization.OptimizationIsPossible == false);
                var existsQuery = indexSearcher.ExistsQuery(field, streamingEnabled: false, forward: true);
                
                // matching lucene results, nulls included
                return indexSearcher.Or(indexSearcher.TermQuery(field, null), existsQuery);
            }
            
            case (IsLeftUnbounded: true, IsRightUnbounded: false):
            {
                Debug.Assert(streamingOptimization.OptimizationIsPossible == false);
                // between null and Value => (oo, x)
                return Build(indexSearcher, index, field, rightValue, rightOperator, ref streamingOptimization);
            }

            case (IsLeftUnbounded: false, IsRightUnbounded: true):
            {
                Debug.Assert(streamingOptimization.OptimizationIsPossible == false);
                // between Value and null => (x, oo)
                var query = Build(indexSearcher, index, field, leftValue, leftOperator, ref streamingOptimization);
                
                var materializedQuery = query switch
                {
                    CoraxBooleanItem bq => bq.Materialize(ref streamingOptimization),
                    _ => query
                };
                
                return indexSearcher.Or(indexSearcher.TermQuery(field, null), materializedQuery);
            }

            case (IsLeftUnbounded: false, IsRightUnbounded: false):
            {
                var fieldHasTime = index.IndexFieldsPersistence.HasTimeValues(field.FieldName.ToString());
                long ticksFromTerm1 = 0L, ticksFromTerm2 = 0L;
                var term1HasTime = fieldHasTime && QueryBuilderHelper.TryGetTime(index, leftValue, out ticksFromTerm1);
                var term2HasTime = fieldHasTime && QueryBuilderHelper.TryGetTime(index, rightValue, out ticksFromTerm2);

                if (term1HasTime && term2HasTime)
                    return new CoraxBooleanItem(indexSearcher, field, ticksFromTerm1, ticksFromTerm2, leftOperator, rightOperator);
        
                // since the field has time values, and time values are indexed in the exact manner,
                // we disable analyzer (matching Lucene behavior) 
                if (term1HasTime || term2HasTime)
                    field = field.ChangeAnalyzer(FieldIndexingMode.Exact);
        
                return new CoraxBooleanItem(indexSearcher, field, leftValue, rightValue, leftOperator, rightOperator);
            }
        }
    }

    public IQueryMatch OptimizeCompoundField(ref CoraxQueryBuilder.StreamingOptimization streamingOptimization)
    {
        switch (Operation)
        {
            case UnaryMatchOperation.Equals:
            {
                Slice startWith = GetStartWithTerm();
                streamingOptimization.SkipOrderByClause = true;
                return _indexSearcher.StartWithQuery(streamingOptimization.CompoundField, startWith, isNegated: false, forward: streamingOptimization.Forward,
                    streamingEnabled: true, validatePostfixLen: true);
            }
            default:
                // TODO: RavenDB-21188
                // TODO: need to implement support for: (Location, Name) compound field
                // TODO: from Users where Location = "Poland" and Name > "Maciej" order by Name
                // TODO: and other range on the _second_ item
                return this;
        }
    }
    
    
    Slice GetStartWithTerm()
    {
        var t = Term;
        if (t is double d)
        {
            t = Bits.DoubleToSortableLong(d);
        }
        if (t is long l)
        {        
            _indexSearcher.Allocator.Allocate(sizeof(long) , out var bs);
            Span<byte> buffer = bs.ToSpan();
            BitConverter.TryWriteBytes(buffer, Bits.SwapBytes(l));
            return new Slice(bs);
        }

        var term = _indexSearcher.EncodeAndApplyAnalyzer(Field, TermAsString).AsSpan();
        _indexSearcher.Allocator.Allocate(term.Length, out var output);

        term.CopyTo(output.ToSpan());
        return new Slice(output);
    }

    
    public IQueryMatch Materialize(ref CoraxQueryBuilder.StreamingOptimization streamingOptimization)
    {
        IQueryMatch baseMatch;

        if (Operation is UnaryMatchOperation.Equals or UnaryMatchOperation.NotEquals)
        {
            IQueryMatch match = Term switch
            {
                long l => _indexSearcher.TermQuery(Field, l),
                double d => _indexSearcher.TermQuery(Field, d),
                _ => _indexSearcher.TermQuery(Field, TermAsString)
            };
                
            if (Operation is UnaryMatchOperation.NotEquals)
                match = _indexSearcher.AndNot(_indexSearcher.AllEntries(), match);
                
            return Boosting is null ? match : _indexSearcher.Boost(match, Boosting.Value);
        }

        bool streamingEnabled = streamingOptimization.SkipOrderByClause;
        bool forwardIterator = (streamingOptimization is {SkipOrderByClause: true, Forward: false}) == false;
        
        if (Operation is UnaryMatchOperation.Between)
        {
            baseMatch = (Term, Term2) switch
            {
                (long l, long l2) => _indexSearcher.BetweenQuery(Field, l, l2, leftSide: BetweenLeft, rightSide: BetweenRight,  forwardIterator, streamingEnabled),
                (double d, double d2) => _indexSearcher.BetweenQuery(Field, d, d2, leftSide: BetweenLeft, rightSide: BetweenRight,  forwardIterator, streamingEnabled),
                (string s, string s2) => _indexSearcher.BetweenQuery(Field, s, s2, leftSide: BetweenLeft, rightSide: BetweenRight,  forwardIterator, streamingEnabled),
                (long l, double d) => _indexSearcher.BetweenQuery(Field, Convert.ToDouble(l), d, leftSide: BetweenLeft, rightSide: BetweenRight,forwardIterator, streamingEnabled),
                (double d, long l) => _indexSearcher.BetweenQuery(Field, d, Convert.ToDouble(l), leftSide: BetweenLeft, rightSide: BetweenRight,forwardIterator, streamingEnabled),
                _ => throw new InvalidOperationException($"UnaryMatchOperation {Operation} is not supported for type {Term.GetType()}")
            };
        }
        else
        {
            baseMatch = (Operation, Term) switch
            {
                (UnaryMatchOperation.LessThan, long term) => _indexSearcher.LessThanQuery(Field, term, forwardIterator, streamingEnabled),
                (UnaryMatchOperation.LessThan, double term) => _indexSearcher.LessThanQuery(Field, term,  forwardIterator, streamingEnabled),
                (UnaryMatchOperation.LessThan, string term) => _indexSearcher.LessThanQuery(Field, term,  forwardIterator, streamingEnabled),

                (UnaryMatchOperation.LessThanOrEqual, long term) => _indexSearcher.LessThanOrEqualsQuery(Field, term,  forwardIterator, streamingEnabled),
                (UnaryMatchOperation.LessThanOrEqual, double term) => _indexSearcher.LessThanOrEqualsQuery(Field, term,  forwardIterator, streamingEnabled),
                (UnaryMatchOperation.LessThanOrEqual, string term) => _indexSearcher.LessThanOrEqualsQuery(Field, term,  forwardIterator, streamingEnabled),

                (UnaryMatchOperation.GreaterThan, long term) => _indexSearcher.GreaterThanQuery(Field, term,  forwardIterator, streamingEnabled),
                (UnaryMatchOperation.GreaterThan, double term) => _indexSearcher.GreaterThanQuery(Field, term, forwardIterator, streamingEnabled),
                (UnaryMatchOperation.GreaterThan, string term) => _indexSearcher.GreaterThanQuery(Field, term, forwardIterator, streamingEnabled),


                (UnaryMatchOperation.GreaterThanOrEqual, long term) => _indexSearcher.GreatThanOrEqualsQuery(Field, term,  forwardIterator, streamingEnabled),
                (UnaryMatchOperation.GreaterThanOrEqual, double term) => _indexSearcher.GreatThanOrEqualsQuery(Field, term,  forwardIterator, streamingEnabled),
                (UnaryMatchOperation.GreaterThanOrEqual, string term) => _indexSearcher.GreatThanOrEqualsQuery(Field, term,  forwardIterator, streamingEnabled),
                _ => throw new ArgumentException("This is only Greater*/Less* Query part")
            };
        }

        return Boosting is null 
            ? baseMatch
            : _indexSearcher.Boost(baseMatch, Boosting.Value);
    }

    public SkipSortingResult AttemptToSkipSorting() => throw new InvalidOperationException(IQueryMatchUsageException);

    public QueryCountConfidence Confidence => throw new InvalidOperationException(IQueryMatchUsageException);
    public int Fill(Span<long> matches) => throw new InvalidOperationException(IQueryMatchUsageException);

    public int AndWith(Span<long> buffer, int matches) => throw new InvalidOperationException(IQueryMatchUsageException);

    public void Score(Span<long> matches, Span<float> scores, float boostFactor) => throw new InvalidOperationException(IQueryMatchUsageException);

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

    public float? Boosting { get; set; }
}
