using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax;
using Corax.Queries;
using Corax.Utils;
using Raven.Server.Documents.Queries;
using Sparrow.Extensions;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

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

    private const string IQueryMatchUsageException =
        $"You tried to use {nameof(CoraxBooleanQuery)} as normal querying function. This class is only for type - relaxation inside {nameof(CoraxQueryBuilder)} to build big UnaryMatch stack";

    private readonly IQueryScoreFunction _scoreFunction;
    private readonly IndexSearcher _indexSearcher;
    public long Count { get; }

    public CoraxBooleanItem(IndexSearcher searcher, string name, int fieldId, object term, UnaryMatchOperation operation,
        IQueryScoreFunction scoreFunction = default)
    {
        _scoreFunction = scoreFunction;
        Name = name;
        FieldId = fieldId;
        Term = term;
        Operation = operation;
        _indexSearcher = searcher;

        Unsafe.SkipInit(out Term2);
        Unsafe.SkipInit(out BetweenLeft);
        Unsafe.SkipInit(out BetweenRight);

        if (operation is UnaryMatchOperation.Equals or UnaryMatchOperation.NotEquals)
        {
            TermAsString = QueryBuilderHelper.CoraxGetValueAsString(term);
            Count = searcher.TermAmount(name, TermAsString);
        }
        else
        {
            Unsafe.SkipInit(out TermAsString);
            Count = searcher.GetEntriesAmountInField(name);
        }
    }

    public CoraxBooleanItem(IndexSearcher searcher, string name, int fieldId, object term1, object term2, UnaryMatchOperation operation, UnaryMatchOperation left,
        UnaryMatchOperation right,
        IQueryScoreFunction scoreFunction = default) : this(searcher, name, fieldId, term1, operation, scoreFunction)
    {
        //Between handler
        Term2 = term2;
        BetweenRight = right;
        BetweenLeft = left;
    }

    public QueryCountConfidence Confidence => throw new InvalidOperationException(IQueryMatchUsageException);
    public bool IsBoosting => _scoreFunction is not NullScoreFunction;
    public int Fill(Span<long> matches) => throw new InvalidOperationException(IQueryMatchUsageException);

    public int AndWith(Span<long> buffer, int matches) => throw new InvalidOperationException(IQueryMatchUsageException);

    public void Score(Span<long> matches, Span<float> scores) => throw new InvalidOperationException(IQueryMatchUsageException);

    public QueryInspectionNode Inspect() => throw new InvalidOperationException(IQueryMatchUsageException);

    public static bool CanBeMerged(CoraxBooleanItem lhs, CoraxBooleanItem rhs)
    {
        return (lhs._scoreFunction, rhs._scoreFunction) switch
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
}

public class CoraxBooleanQuery : IQueryMatch
{
    private readonly List<CoraxBooleanItem> _queryStack;
    private readonly IQueryScoreFunction _scoreFunction;

    private const string QueryMatchUsageException =
        $"You tried to use {nameof(CoraxBooleanQuery)} as normal querying function. This class is only for type - relaxation inside {nameof(CoraxQueryBuilder)} to build big UnaryMatch stack";

    private MemoizationMatchProviderRef<AllEntriesMatch> _allEntries;

    private readonly IndexSearcher _indexSearcher;
    public bool HasInnerBinary { get; private set; }

    public CoraxBooleanQuery(IndexSearcher indexSearcher, MemoizationMatchProviderRef<AllEntriesMatch> allEntries, CoraxBooleanItem left, CoraxBooleanItem right,
        IQueryScoreFunction scoreFunction)
    {
        _indexSearcher = indexSearcher;
        _queryStack = new List<CoraxBooleanItem>() {left, right};
        _scoreFunction = scoreFunction;
        _allEntries = allEntries;
    }

    public bool TryMerge(CoraxBooleanQuery other)
    {
        if (other._scoreFunction.GetType() != _scoreFunction.GetType())
            return false;

        _queryStack.AddRange(other._queryStack);
        return true;
    }

    public bool TryAnd(CoraxBooleanItem item)
    {
        if (CoraxBooleanItem.CanBeMerged(_queryStack[0], item) == false)
            return false;

        _queryStack.Add(item);

        return true;
    }

    public unsafe IQueryMatch Materialize()
    {
        Debug.Assert(_queryStack.Count > 0);

        _queryStack.Sort(((firstUnaryItem, secondUnaryItem) =>
        {
            if (firstUnaryItem.Operation == UnaryMatchOperation.Equals && secondUnaryItem.Operation != UnaryMatchOperation.Equals)
                return -1;
            if (firstUnaryItem.Operation != UnaryMatchOperation.Equals && secondUnaryItem.Operation == UnaryMatchOperation.Equals)
                return 1;
            if (firstUnaryItem.Operation == UnaryMatchOperation.Between && secondUnaryItem.Operation != UnaryMatchOperation.Between)
                return -1;
            if (firstUnaryItem.Operation != UnaryMatchOperation.Between && secondUnaryItem.Operation == UnaryMatchOperation.Between)
                return 1;
            if (firstUnaryItem.Operation == UnaryMatchOperation.Between && secondUnaryItem.Operation == UnaryMatchOperation.Between)
                return firstUnaryItem.Count.CompareTo(secondUnaryItem.Count);

            return firstUnaryItem.Count.CompareTo(secondUnaryItem.Count);
        }));

        IQueryMatch baseMatch = null;
        var stack = CollectionsMarshal.AsSpan(_queryStack);
        int reduced = 0;
        foreach (var query in stack)
        {
            //Out of TermMatches in our stack
            if (query.Operation is not (UnaryMatchOperation.Equals or UnaryMatchOperation.NotEquals))
                break;

            //We're always do TermMatch (true and NOT (X))
            IQueryMatch second = _indexSearcher.TermQuery(query.Name, query.TermAsString, query.FieldId);

            if (query.Operation is UnaryMatchOperation.NotEquals)
            {
                //This could be more expensive than scanning RAW elements. This returns ~(field.NumberOfEntries - term.NumberOfEntries). Can we set threshold around ~10% to perfom SCAN maybe? 
                if (baseMatch != null)
                {
                    // Instead of performing AND(TermMatch, AndNot(Exist, Term)) we can translate it into AndNot(baseMatch, Term). This way we avoid additional BinaryMatch
                    baseMatch = _indexSearcher.AndNot(baseMatch, second);
                    HasInnerBinary = true;
                    goto Reduce;
                }

                //In the first place we've to do (true and NOT))
                baseMatch = _indexSearcher.AndNot<MultiTermMatch, TermMatch>(_indexSearcher.ExistsQuery(query.Name), (TermMatch)second);
                HasInnerBinary = true;
                goto Reduce;
            }


            // TermMatch:
            // This should be more complex. Should we always perform AND for TermMatch? For example there could be a case when performing RangeQueries in first place will limit our set very well so scanning would be better option.

            
            if (baseMatch == null)
            {
                baseMatch = second;
            }
            else
            {
                baseMatch = _indexSearcher.And(baseMatch, second);
                HasInnerBinary = true;
            }
            Reduce:
            reduced++;

        }

        stack = stack.Slice(reduced);
        if (stack.Length == 0)
            goto Return;

        // Ascending term amount stack. It not always would work for us. Thats is telling us terms inside field are clustered in centrains points
        var leftmostClause = stack[0];
        if (leftmostClause.Operation is UnaryMatchOperation.Between)
        {
            var nextQuery = (leftmostClause.Term, leftmostClause.Term2) switch
            {
                (long l, long l2) => _indexSearcher.BetweenQuery(leftmostClause.Name, l, l2,
                    default(NullScoreFunction),
                    leftSide: leftmostClause.BetweenLeft, rightSide: leftmostClause.BetweenRight, fieldId: leftmostClause.FieldId),
                (double d, double d2) => _indexSearcher.BetweenQuery(leftmostClause.Name, d, d2,
                    default(NullScoreFunction),
                    leftSide: leftmostClause.BetweenLeft, rightSide: leftmostClause.BetweenRight, fieldId: leftmostClause.FieldId),
                (string s, string s2) => _indexSearcher.BetweenQuery(leftmostClause.Name, s, s2,
                    default(NullScoreFunction),
                    leftSide: leftmostClause.BetweenLeft, rightSide: leftmostClause.BetweenRight, fieldId: leftmostClause.FieldId),
                (long l, double d) => _indexSearcher.BetweenQuery(leftmostClause.Name, Convert.ToDouble(l), d,
                    default(NullScoreFunction), leftSide: leftmostClause.BetweenLeft, rightSide: leftmostClause.BetweenRight, fieldId: leftmostClause.FieldId),
                (double d, long l) => _indexSearcher.BetweenQuery(leftmostClause.Name, d, Convert.ToDouble(l),
                    default(NullScoreFunction), leftSide: leftmostClause.BetweenLeft, rightSide: leftmostClause.BetweenRight, fieldId: leftmostClause.FieldId),
                _ => throw new InvalidOperationException($"UnaryMatchOperation {leftmostClause.Operation} is not supported for type {leftmostClause.Term.GetType()}")
            };

            baseMatch = baseMatch is null
                ? nextQuery
                : _indexSearcher.And(baseMatch, nextQuery);
        }
        else
        {
            var nextQuery = (leftmostClause.Operation, leftmostClause.Term) switch
            {
                (UnaryMatchOperation.LessThan, long l) => _indexSearcher.LessThanQuery(leftmostClause.Name, l, default(NullScoreFunction),
                    fieldId: leftmostClause.FieldId),
                (UnaryMatchOperation.LessThan, double d) => _indexSearcher.LessThanQuery(leftmostClause.Name, d, default(NullScoreFunction),
                    fieldId: leftmostClause.FieldId),
                (UnaryMatchOperation.LessThan, string s) => _indexSearcher.LessThanQuery(leftmostClause.Name, s, default(NullScoreFunction),
                    fieldId: leftmostClause.FieldId),

                (UnaryMatchOperation.LessThanOrEqual, long l) => _indexSearcher.LessThanOrEqualsQuery(leftmostClause.Name, l, default(NullScoreFunction),
                    fieldId: leftmostClause.FieldId),
                (UnaryMatchOperation.LessThanOrEqual, double d) => _indexSearcher.LessThanOrEqualsQuery(leftmostClause.Name, d, default(NullScoreFunction),
                    fieldId: leftmostClause.FieldId),
                (UnaryMatchOperation.LessThanOrEqual, string s) => _indexSearcher.LessThanOrEqualsQuery(leftmostClause.Name, s, default(NullScoreFunction),
                    fieldId: leftmostClause.FieldId),

                (UnaryMatchOperation.GreaterThan, long l) => _indexSearcher.GreaterThanQuery(leftmostClause.Name, l, default(NullScoreFunction),
                    fieldId: leftmostClause.FieldId),
                (UnaryMatchOperation.GreaterThan, double d) => _indexSearcher.GreaterThanQuery(leftmostClause.Name, d, default(NullScoreFunction),
                    fieldId: leftmostClause.FieldId),
                (UnaryMatchOperation.GreaterThan, string s) => _indexSearcher.GreaterThanQuery(leftmostClause.Name, s, default(NullScoreFunction),
                    fieldId: leftmostClause.FieldId),

                (UnaryMatchOperation.GreaterThanOrEqual, long l) => _indexSearcher.GreatThanOrEqualsQuery(leftmostClause.Name, l, default(NullScoreFunction),
                    fieldId: leftmostClause.FieldId),
                (UnaryMatchOperation.GreaterThanOrEqual, double d) => _indexSearcher.GreatThanOrEqualsQuery(leftmostClause.Name, d, default(NullScoreFunction),
                    fieldId: leftmostClause.FieldId),
                (UnaryMatchOperation.GreaterThanOrEqual, string s) => _indexSearcher.GreatThanOrEqualsQuery(leftmostClause.Name, s, default(NullScoreFunction),
                    fieldId: leftmostClause.FieldId),
                _ => throw new InvalidOperationException($"UnaryMatchOperation {leftmostClause.Operation} is not supported for type {leftmostClause.Term.GetType()}")            };

            baseMatch = baseMatch is null
                ? nextQuery
                : _indexSearcher.And(baseMatch, nextQuery);
        }

        MultiUnaryItem[] listOfMergedUnaries = new MultiUnaryItem[stack.Length - 1];
        for (var index = 1; index < stack.Length; index++)
        {
            var query = stack[index];
            if (query.Operation is UnaryMatchOperation.Between)
            {
                listOfMergedUnaries[index - 1] = (query.Term, query.Term2) switch
                {
                    (long l, long l2) => new MultiUnaryItem(query.FieldId, l, l2, query.BetweenLeft, query.BetweenRight),
                    (double d, double d2) => new MultiUnaryItem(query.FieldId, d, d2, query.BetweenLeft, query.BetweenRight),
                    (string s, string s2) => new MultiUnaryItem(_indexSearcher, query.FieldId, s, s2, query.BetweenLeft, query.BetweenRight),
                    (long l, double d) => new MultiUnaryItem(query.FieldId, Convert.ToDouble(l), d, query.BetweenLeft, query.BetweenRight),
                    (double d, long l) => new MultiUnaryItem(query.FieldId, d, Convert.ToDouble(l), query.BetweenLeft, query.BetweenRight),
                    _ => throw new InvalidOperationException($"UnaryMatchOperation {query.Operation} is not supported for type {query.Term.GetType()}")
                };
            }
            else
            {
                listOfMergedUnaries[index - 1] = query.Term switch
                {
                    long longTerm => new MultiUnaryItem(query.FieldId, longTerm, query.Operation),
                    double doubleTerm => new MultiUnaryItem(query.FieldId, doubleTerm, query.Operation),
                    _ => new MultiUnaryItem(_indexSearcher, query.FieldId, query.Term as string, query.Operation),
                };
            }
        }


        if (listOfMergedUnaries.Length > 0)
        {
            baseMatch = _indexSearcher.CreateMultiUnaryMatch(baseMatch ?? _indexSearcher.ExistsQuery(stack[1].Name), listOfMergedUnaries);
        }

        Return:
        return _scoreFunction is NullScoreFunction
            ? baseMatch
            : _indexSearcher.Boost(baseMatch, _scoreFunction);
    }

    public long Count => throw new InvalidOperationException(QueryMatchUsageException);

    public QueryCountConfidence Confidence => throw new InvalidOperationException(QueryMatchUsageException);

    public bool IsBoosting => _scoreFunction is not NullScoreFunction;
    public int Fill(Span<long> matches) => throw new InvalidOperationException(QueryMatchUsageException);

    public int AndWith(Span<long> buffer, int matches) => throw new InvalidOperationException(QueryMatchUsageException);

    public void Score(Span<long> matches, Span<float> scores) => throw new InvalidOperationException(QueryMatchUsageException);

    public QueryInspectionNode Inspect() => throw new InvalidOperationException(QueryMatchUsageException);
}

public class CostCounter
{
    private CoraxBooleanItem[] _list;

    /// <summary>
    /// Assumption: this is bigger than 0;
    /// </summary>
    private readonly long _numberOfEntries;

    private double[] _ratio;

    public CostCounter(IndexSearcher searcher, CoraxBooleanItem[] list)
    {
        _list = list;
        _numberOfEntries = searcher.NumberOfEntries;
        _ratio = ArrayPool<double>.Shared.Rent(list.Length);
    }

    // Main goal is to find smallest beginning set to perform operations on it.
    // This class is not used for know.

    public void Optimize()
    {
        // At very beginning lets find hit ratio of our term
        for (int index = 0; index < _list.Length; index++)
        {
            CoraxBooleanItem item = _list[index];
            ref var ratio = ref _ratio[index];

            //Closer to 1 (or over) means we should avoid it. 
            if (item.Operation == UnaryMatchOperation.Equals)
                ratio = (double)item.Count / _numberOfEntries;
            else if (item.Operation == UnaryMatchOperation.NotEquals)
                ratio = (double)(_numberOfEntries - item.Count) / _numberOfEntries;
            else
            {
                //This is ideal scenario IF they are ranged (not (-inf, X) or (X, inf)
                //Values over 1 means duplications
                var count = item.Materialize().Count;
            }

            //This is interesting part because we don't know how many items do we hit. Maybe should we go trough tree and calculate the cost first? 
        }
    }


    public ReadOnlySpan<CoraxBooleanItem> GetStack() => _list.AsSpan();
}
