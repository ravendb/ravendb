using System.Runtime.CompilerServices;


namespace Corax.Queries
{
    public unsafe struct BinaryMatch : IQueryMatch
    {
        private readonly FunctionTable _functionTable;
        private IQueryMatch _inner;

        internal BinaryMatch(IQueryMatch match, FunctionTable functionTable)
        {
            _inner = match;
            _functionTable = functionTable;
        }

        public long Count => _functionTable.CountFunc(ref this);

        public long Current => _functionTable.CurrentFunc(ref this);


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SeekTo(long next = 0)
        {
            return _functionTable.SeekToFunc(ref this, next);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveNext(out long v)
        {
            return _functionTable.MoveNextFunc(ref this, out v);
        }

        internal class FunctionTable
        {
            public readonly delegate*<ref BinaryMatch, long, bool> SeekToFunc;
            public readonly delegate*<ref BinaryMatch, out long, bool> MoveNextFunc;
            public readonly delegate*<ref BinaryMatch, long> CountFunc;
            public readonly delegate*<ref BinaryMatch, long> CurrentFunc;

            public FunctionTable(
                delegate*<ref BinaryMatch, long, bool> seekToFunc,
                delegate*<ref BinaryMatch, out long, bool> moveNextFunc,
                delegate*<ref BinaryMatch, long> countFunc,
                delegate*<ref BinaryMatch, long> currentFunc)
            {
                SeekToFunc = seekToFunc;
                MoveNextFunc = moveNextFunc;
                CountFunc = countFunc;
                CurrentFunc = currentFunc;
            }
        }

        private static class StaticFunctionCache<TInner, TOuter>
            where TInner : IQueryMatch
            where TOuter : IQueryMatch
        {
            public static readonly FunctionTable FunctionTable;

            static StaticFunctionCache()
            {
                static long CountFunc(ref BinaryMatch match)
                {
                    return ((BinaryMatch<TInner, TOuter>)match._inner).Count;
                }
                static long CurrentFunc(ref BinaryMatch match)
                {
                    return ((BinaryMatch<TInner, TOuter>)match._inner).Current;
                }
                static bool SeekToFunc(ref BinaryMatch match, long v)
                {
                    if (match._inner is BinaryMatch<TInner, TOuter> inner)
                    {
                        var result = inner.SeekTo(v);
                        match._inner = inner;
                        return result;
                    }
                    return false;
                }
                static bool MoveNextFunc(ref BinaryMatch match, out long v)
                {
                    if (match._inner is BinaryMatch<TInner, TOuter> inner)
                    {
                        var result = inner.MoveNext(out v);
                        match._inner = inner;
                        return result;
                    }
                    Unsafe.SkipInit(out v);
                    return false;
                }

                FunctionTable = new FunctionTable(&SeekToFunc, &MoveNextFunc, &CountFunc, &CurrentFunc);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BinaryMatch Create<TInner, TOuter>(in BinaryMatch<TInner, TOuter> query)
            where TInner : IQueryMatch
            where TOuter : IQueryMatch
        {
            return new BinaryMatch(query, StaticFunctionCache<TInner, TOuter>.FunctionTable);
        }
    }
}
