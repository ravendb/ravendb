using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax;
using Corax.Queries;
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
            TermAsString = QueryBuilderHelper.CoraxGetValueAsString(term);
        else
            Unsafe.SkipInit(out TermAsString);

        Count = operation is UnaryMatchOperation.Equals or UnaryMatchOperation.NotEquals
            ? searcher.TermAmount(name, TermAsString)
            : searcher.GetEntriesAmountInField(name);
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
    public bool IsBoosting => throw new InvalidOperationException(IQueryMatchUsageException);
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
                (long l, long l2) => _indexSearcher.Between(_indexSearcher.ExistsQuery(Name), FieldId, l, l2, BetweenLeft, BetweenRight),
                (double d, double d2) => _indexSearcher.Between(_indexSearcher.ExistsQuery(Name), FieldId, d, d2, BetweenLeft, BetweenRight),
                (string s, string s2) => _indexSearcher.Between(_indexSearcher.ExistsQuery(Name), FieldId, s, s2, BetweenLeft, BetweenRight),
                _ => throw new InvalidOperationException($"UnaryMatchOperation {Operation} is not supported for type {Term.GetType()}")
            };
        }

        else
        {
            baseMatch = Term switch
            {
                long l => _indexSearcher.UnaryQuery(_indexSearcher.ExistsQuery(Name), FieldId, l, Operation),
                double d => _indexSearcher.UnaryQuery(_indexSearcher.ExistsQuery(Name), FieldId, d, Operation),
                string s => _indexSearcher.UnaryQuery(_indexSearcher.ExistsQuery(Name), FieldId, s, Operation),
                _ => throw new InvalidOperationException($"UnaryMatchOperation {Operation} is not supported for type {Term.GetType()}")
            };
        }

        return _scoreFunction is NullScoreFunction
            ? baseMatch
            : _indexSearcher.Boost(baseMatch, _scoreFunction);
    }
}

public class CoraxBooleanQuery : IQueryMatch
{
    private readonly List<CoraxBooleanItem> _queryStack;
    private readonly IQueryScoreFunction _scoreFunction;

    private const string QueryMatchUsageException =
        $"You tried to use {nameof(CoraxBooleanQuery)} as normal querying function. This class is only for type - relaxation inside {nameof(CoraxQueryBuilder)} to build big UnaryMatch stack";

    private MemoizationMatchProvider<AllEntriesMatch> _allEntries;

    private readonly IndexSearcher _indexSearcher;

    public CoraxBooleanQuery(IndexSearcher indexSearcher, MemoizationMatchProvider<AllEntriesMatch> allEntries, CoraxBooleanItem left, CoraxBooleanItem right,
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

    public IQueryMatch Materialize()
    {
        Debug.Assert(_queryStack.Count > 0);

        _queryStack.Sort(((item, booleanItem) =>
        {
            if (item.Operation == UnaryMatchOperation.Equals && booleanItem.Operation != UnaryMatchOperation.Equals)
                return -1;
            if (item.Operation != UnaryMatchOperation.Equals && booleanItem.Operation == UnaryMatchOperation.Equals)
                return 1;


            return item.Count.CompareTo(booleanItem.Count);
        }));

        IQueryMatch baseMatch = null;
        var stack = CollectionsMarshal.AsSpan(_queryStack);
        int reduced = 0;
        foreach (var query in stack)
        {
            if (query.Operation is not (UnaryMatchOperation.Equals or UnaryMatchOperation.NotEquals))
                break;
            
            IQueryMatch second = _indexSearcher.TermQuery(query.Name, query.TermAsString, query.FieldId);
                if (query.Operation is UnaryMatchOperation.NotEquals)
                    second = _indexSearcher.AndNot(
                        _indexSearcher.ExistsQuery(query.Name),
                        second
                    );
                ;

            if (baseMatch == null)
                baseMatch = second;
            else
                baseMatch = _indexSearcher.And(baseMatch, second);

            reduced++;
        }

        stack = stack.Slice(reduced);

        foreach (var query in stack)
        {
            if (query.Operation is UnaryMatchOperation.Between)
            {
                baseMatch = (query.Term, query.Term2) switch
                {
                    (long l, long l2) => _indexSearcher.Between(baseMatch ?? _indexSearcher.ExistsQuery(query.Name), query.FieldId, l, l2, query.BetweenLeft,
                        query.BetweenRight),
                    (double d, double d2) => _indexSearcher.Between(baseMatch ?? _indexSearcher.ExistsQuery(query.Name), query.FieldId, d, d2, query.BetweenLeft,
                        query.BetweenRight),
                    (string s, string s2) => _indexSearcher.Between(baseMatch ?? _indexSearcher.ExistsQuery(query.Name), query.FieldId, s, s2, query.BetweenLeft,
                        query.BetweenRight),
                    _ => throw new InvalidOperationException($"UnaryMatchOperation {query.Operation} is not supported for type {query.Term.GetType()}")
                };
            }
            else
            {
                baseMatch = query.Term switch
                {
                    long longTerm => _indexSearcher.UnaryQuery(baseMatch ?? _indexSearcher.ExistsQuery(query.Name), query.FieldId, longTerm, query.Operation, -1),
                    double doubleTerm => _indexSearcher.UnaryQuery(baseMatch ?? _indexSearcher.ExistsQuery(query.Name), query.FieldId, doubleTerm, query.Operation, -1),
                    _ => _indexSearcher.UnaryQuery(baseMatch ?? _indexSearcher.ExistsQuery(query.Name), query.FieldId, query.Term as string, query.Operation, -1),
                };
            }
        }

        return _scoreFunction is NullScoreFunction
            ? baseMatch
            : _indexSearcher.Boost(baseMatch, _scoreFunction);
    }

    public long Count => throw new InvalidOperationException(QueryMatchUsageException);

    public QueryCountConfidence Confidence => throw new InvalidOperationException(QueryMatchUsageException);

    public bool IsBoosting => throw new InvalidOperationException(QueryMatchUsageException);

    public int Fill(Span<long> matches) => throw new InvalidOperationException(QueryMatchUsageException);

    public int AndWith(Span<long> buffer, int matches) => throw new InvalidOperationException(QueryMatchUsageException);

    public void Score(Span<long> matches, Span<float> scores) => throw new InvalidOperationException(QueryMatchUsageException);

    public QueryInspectionNode Inspect() => throw new InvalidOperationException(QueryMatchUsageException);
}
