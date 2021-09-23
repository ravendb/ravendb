using System;
using System.Runtime.CompilerServices;
using Voron;

namespace Corax.Queries
{
    public enum UnaryMatchOperation
    {
        GreaterThan,
        GreaterThanOrEqual,
        LessThan,
        LessThanOrEqual,
        Equal,
        NotEqual,
        Not
    }

    public unsafe partial struct UnaryMatch<TInner, TValueType> : IQueryMatch
        where TInner : struct, IQueryMatch
    {
        private readonly delegate*<ref UnaryMatch<TInner, TValueType>, Span<long>, int> _fillFunc;
        private readonly delegate*<ref UnaryMatch<TInner, TValueType>, Span<long>, int> _andWith;
        private TInner _inner;
        private UnaryMatchOperation _operation;
        private readonly IndexSearcher _searcher;
        private readonly int _fieldId;
        private readonly TValueType _value;

        private long _totalResults;
        private long _current;
        private QueryCountConfidence _confidence;

        public long Count => _totalResults;
        public long Current => _current;

        public QueryCountConfidence Confidence => _confidence;

        private UnaryMatch(in TInner inner,
            UnaryMatchOperation operation,
            IndexSearcher searcher,
            int fieldId,
            TValueType value,
            delegate*<ref UnaryMatch<TInner, TValueType>, Span<long>, int> fillFunc,
            delegate*<ref UnaryMatch<TInner, TValueType>, Span<long>, int> andWith,
            long totalResults,
            QueryCountConfidence confidence)
        {
            _totalResults = totalResults;
            _current = QueryMatch.Start;
            _fillFunc = fillFunc;
            _andWith = andWith;
            _inner = inner;
            _operation = operation;
            _searcher = searcher;
            _fieldId = fieldId;
            _value = value;
            _confidence = confidence;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> buffer)
        {
            return _fillFunc(ref this, buffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer)
        {
            return _andWith(ref this, buffer);
        }
    }
}
