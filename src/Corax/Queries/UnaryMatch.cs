using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Voron;

namespace Corax.Queries
{
    public enum UnaryMatchOperation
    {
        GreaterThan,
        GreaterThanOrEqual,
        LessThan,
        LessThanOrEqual,
        Equals,
        NotEquals,
        Between,
        NotBetween,
        AllIn,
        Unknown
    }

    public enum UnaryMatchOperationMode
    {
        Any,
        All
    }

    [DebuggerDisplay("{DebugView,nq}")]
    public unsafe partial struct UnaryMatch<TInner, TValueType> : IQueryMatch
        where TInner : IQueryMatch
    {
        private readonly delegate*<ref UnaryMatch<TInner, TValueType>, Span<long>, int> _fillFunc;
        private readonly delegate*<ref UnaryMatch<TInner, TValueType>, Span<long>, int, int> _andWithFunc;

        private TInner _inner;
        private UnaryMatchOperation _operation;
        private readonly IndexSearcher _searcher;
        private readonly FieldMetadata _field;
        private readonly TValueType _value;
        private readonly TValueType _valueAux;
        private readonly int _take;
        private readonly UnaryMatchOperationMode _operationMode;
        
        private long _totalResults;
        private long _current;
        private QueryCountConfidence _confidence;

        public bool IsBoosting => _inner.IsBoosting;
        public long Count => _totalResults;
        public long Current => _current;

        public QueryCountConfidence Confidence => _confidence;

        private UnaryMatch(in TInner inner,
            UnaryMatchOperation operation,
            IndexSearcher searcher,
            FieldMetadata field,
            TValueType value,
            delegate*<ref UnaryMatch<TInner, TValueType>, Span<long>, int> fillFunc,
            delegate*<ref UnaryMatch<TInner, TValueType>, Span<long>, int, int> andWithFunc,
            long totalResults,
            QueryCountConfidence confidence,
            UnaryMatchOperationMode mode = UnaryMatchOperationMode.Any,
            int take = -1)
        {
            _totalResults = totalResults;
            _current = QueryMatch.Start;

            _fillFunc = fillFunc;
            _andWithFunc = andWithFunc;

            _inner = inner;
            _operation = operation;
            _searcher = searcher;
            _field = field;
            _value = value;
            _valueAux = default;
            _confidence = confidence;
            _operationMode = mode;
            _take = take <= 0 ? int.MaxValue : take;
        }

        private UnaryMatch(in TInner inner,
            UnaryMatchOperation operation,
            IndexSearcher searcher,
            FieldMetadata field,
            TValueType value1,
            TValueType value2,
            delegate*<ref UnaryMatch<TInner, TValueType>, Span<long>, int> fillFunc,
            delegate*<ref UnaryMatch<TInner, TValueType>, Span<long>, int, int> andWith,
            long totalResults,
            QueryCountConfidence confidence,
            UnaryMatchOperationMode mode = UnaryMatchOperationMode.Any,
            int take = -1)
        {
            _totalResults = totalResults;
            _current = QueryMatch.Start;

            _fillFunc = fillFunc;
            _andWithFunc = andWith;

            _inner = inner;
            _operation = operation;
            _searcher = searcher;
            _field = field;
            _value = value1;
            _valueAux = value2;
            _confidence = confidence;
            _operationMode = mode;
            _take = take <= 0 ? int.MaxValue : take;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> buffer)
        {
            return _fillFunc(ref this, buffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer, int matches)
        {
            return _andWithFunc(ref this, buffer, matches);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Score(Span<long> matches, Span<float> scores, float boostFactor) 
        {
            _inner.Score(matches, scores, boostFactor);
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(UnaryMatch)} [{_operation}]",
                children: new List<QueryInspectionNode> { _inner.Inspect() },
                parameters: new Dictionary<string, string>()
                {
                    { nameof(IsBoosting), IsBoosting.ToString() },
                    { nameof(Count), $"{Count} [{Confidence}]" },
                    { "Operation", _operation.ToString() },
                    { "Field", $"{_field.ToString()}" },
                    { "Value", GetValue()},
                    { "AuxValue", GetAuxValue()}
                });
        }

        private string GetValue()
        {
            return ((object)_value) switch
            {
                long => ((long)(object)_value).ToString(),
                double => ((double)(object)_value).ToString(CultureInfo.InvariantCulture),
                Slice => ((Slice)(object)_value).ToString(),
                _ => "Unknown type of value"
            };
        }
        private string GetAuxValue()
        {
            return ((object)_value) switch
            {
                long => ((long)(object)_valueAux).ToString(),
                double => ((double)(object)_valueAux).ToString(CultureInfo.InvariantCulture),
                Slice => ((Slice)(object)_valueAux).ToString(),
                _ => "Unknown type of value aux"
            };
        }

        string DebugView => Inspect().ToString();
    }
}
