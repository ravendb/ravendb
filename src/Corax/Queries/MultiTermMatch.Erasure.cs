using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Corax.Queries
{
    public unsafe struct MultiTermMatch : IQueryMatch
    {
        private readonly FunctionTable _functionTable;
        private IQueryMatch _inner;

        internal MultiTermMatch(IQueryMatch match, FunctionTable functionTable)
        {
            _inner = match;
            _functionTable = functionTable;
        }

        public long Count => _functionTable.CountFunc(ref this);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> buffer)
        {
            return _functionTable.FillFunc(ref this, buffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer)
        {
            return _functionTable.AndWithFunc(ref this, buffer);
        }

        internal class FunctionTable
        {
            public readonly delegate*<ref MultiTermMatch, Span<long>, int> FillFunc;
            public readonly delegate*<ref MultiTermMatch, Span<long>, int> AndWithFunc;
            public readonly delegate*<ref MultiTermMatch, long> CountFunc;

            public FunctionTable(
                delegate*<ref MultiTermMatch, Span<long>, int> fillFunc,
                delegate*<ref MultiTermMatch, Span<long>, int> andWithFunc,
                delegate*<ref MultiTermMatch, long> countFunc)
            {
                FillFunc = fillFunc;
                AndWithFunc = andWithFunc;
                CountFunc = countFunc;
            }
        }

        private static class StaticFunctionCache<TInner>
            where TInner : ITermProvider
        {
            public static readonly FunctionTable FunctionTable;

            static StaticFunctionCache()
            {
                static long CountFunc(ref MultiTermMatch match)
                {
                    return ((MultiTermMatch<TInner>)match._inner).Count;
                }
                static int FillFunc(ref MultiTermMatch match, Span<long> matches)
                {
                    if (match._inner is MultiTermMatch<TInner> inner)
                    {
                        var result = inner.Fill(matches);
                        match._inner = inner;
                        return result;
                    }
                    return 0;
                }

                static int AndWithFunc(ref MultiTermMatch match, Span<long> matches)
                {
                    if (match._inner is MultiTermMatch<TInner> inner)
                    {
                        var result = inner.AndWith(matches);
                        match._inner = inner;
                        return result;
                    }
                    return 0;
                }

                FunctionTable = new FunctionTable(&FillFunc, &AndWithFunc, &CountFunc);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static MultiTermMatch Create<TInner>(in MultiTermMatch<TInner> query)
             where TInner : ITermProvider
        {
            return new MultiTermMatch(query, StaticFunctionCache<TInner>.FunctionTable);
        }

        private static FunctionTable _binaryFunctions;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static MultiTermMatch Create(in BinaryMatch query)
        {
            static long CountFunc(ref MultiTermMatch match)
            {
                return ((BinaryMatch)match._inner).Count;
            }
            static int FillFunc(ref MultiTermMatch match, Span<long> matches)
            {
                if (match._inner is BinaryMatch inner)
                {
                    var result = inner.Fill(matches);
                    match._inner = inner;
                    return result;
                }
                return 0;
            }

            static int AndWithFunc(ref MultiTermMatch match, Span<long> matches)
            {
                if (match._inner is BinaryMatch inner)
                {
                    var result = inner.AndWith(matches);
                    match._inner = inner;
                    return result;
                }
                return 0;
            }

            if (_binaryFunctions == null)
                _binaryFunctions = new FunctionTable(&FillFunc, &AndWithFunc, &CountFunc);

            return new MultiTermMatch(query, _binaryFunctions);
        }

        private struct EmptyTermProvider : ITermProvider
        {
            public int TermsCount => 0;

            public bool Evaluate(long id)
            {
                throw new NotSupportedException();
            }

            public bool Next(out TermMatch term)
            {
                Unsafe.SkipInit(out term);
                return false;
            }

            public void Reset(){}
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static MultiTermMatch CreateEmpty()
        {
            return new MultiTermMatch(new MultiTermMatch<EmptyTermProvider>(new EmptyTermProvider()), StaticFunctionCache<EmptyTermProvider>.FunctionTable);
        }
    }
}
