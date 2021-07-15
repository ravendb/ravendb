using System;
using System.Runtime.CompilerServices;


namespace Corax.Queries
{
    public unsafe struct BinaryMatch<TInner, TOuter> : IQueryMatch
        where TInner : IQueryMatch
        where TOuter : IQueryMatch
    {
        private readonly delegate*<ref BinaryMatch<TInner, TOuter>, long, bool> _seekToFunc;
        private readonly delegate*<ref BinaryMatch<TInner, TOuter>, out long, bool> _moveNext;

        private TInner _inner;
        private TOuter _outer;

        private long _totalResults;
        private long _current;

        public long Count => _totalResults;
        public long Current => _current;

        private BinaryMatch(in TInner inner, in TOuter outer, delegate*<ref BinaryMatch<TInner, TOuter>, long, bool> seekFunc, delegate*<ref BinaryMatch<TInner, TOuter>, out long, bool> moveNext, long totalResults)
        {
            _totalResults = totalResults;
            _current = QueryMatch.Start;
            _seekToFunc = seekFunc;
            _moveNext = moveNext;
            _inner = inner;
            _outer = outer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool SeekTo(long next = 0)
        {
            return _seekToFunc(ref this, next);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveNext(out long v)
        {
            return _moveNext(ref this, out v);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BinaryMatch<TInner, TOuter> YieldAnd(in TInner inner, in TOuter outer)
        {
            static bool SeekToFunc(ref BinaryMatch<TInner, TOuter> match, long v)
            {
                ref var inner = ref match._inner;
                ref var outer = ref match._outer;

                return inner.SeekTo(v) && outer.SeekTo(v);
            }

            static bool MoveNextFunc(ref BinaryMatch<TInner, TOuter> match, out long v)
            {
                ref var inner = ref match._inner;
                ref var outer = ref match._outer;

                if (inner.Current == QueryMatch.Invalid || outer.Current == QueryMatch.Invalid)
                    goto Fail;

                // Last were equal, moving forward. 
                inner.MoveNext(out v);
                outer.MoveNext(out v);

                while (inner.Current != outer.Current)
                {
                    if (inner.Current < outer.Current)
                    {
                        if (inner.MoveNext(out v) == false)
                            goto Fail;
                    }
                    else
                    {
                        if (outer.MoveNext(out v) == false)
                            goto Fail;
                    }
                }

                // PERF: We dont need to check both as the equal later will take care of that. 
                if (inner.Current == QueryMatch.Invalid)
                    goto Fail;

                if (inner.Current == outer.Current)
                {
                    v = inner.Current;
                    match._current = v;
                    return true;
                }

            Fail:
                match._current = QueryMatch.Invalid;
                v = QueryMatch.Invalid;
                return false;
            }

            return new BinaryMatch<TInner, TOuter>(in inner, in outer, &SeekToFunc, &MoveNextFunc, Math.Min(inner.Count, outer.Count));
        }

        public static BinaryMatch<TInner, TOuter> YieldOr(in TInner inner, in TOuter outer)
        {
            static bool SeekToFunc(ref BinaryMatch<TInner, TOuter> match, long v)
            {
                ref var inner = ref match._inner;
                ref var outer = ref match._outer;

                return inner.SeekTo(v) && outer.SeekTo(v);
            }

            static bool MoveNextFunc(ref BinaryMatch<TInner, TOuter> match, out long v)
            {
                ref var inner = ref match._inner;
                ref var outer = ref match._outer;

                // Nothing else left to add
                if (inner.Current == QueryMatch.Invalid && outer.Current == QueryMatch.Invalid)
                {
                    v = QueryMatch.Invalid;
                    goto Done;
                }
                else if (inner.Current == QueryMatch.Invalid)
                {
                    outer.MoveNext(out v);
                    goto Done;
                }
                else if (outer.Current == QueryMatch.Invalid)
                {
                    inner.MoveNext(out v);
                    goto Done;
                }

                long x, y;
                if (inner.Current == outer.Current)
                {
                    inner.MoveNext(out x);
                    outer.MoveNext(out y);
                }
                else if (inner.Current < outer.Current)
                {
                    inner.MoveNext(out x);
                    y = outer.Current;
                }
                else
                {
                    x = inner.Current;
                    outer.MoveNext(out y);
                }

                if (x == QueryMatch.Invalid && y == QueryMatch.Invalid)
                {
                    v = QueryMatch.Invalid;
                    match._current = QueryMatch.Invalid;
                    return false;
                }
                else if (x == QueryMatch.Invalid)
                {
                    v = y;
                }
                else if (y == QueryMatch.Invalid)
                {
                    v = x;
                }
                else
                {
                    v = x < y ? x : y;
                }

            Done:
                match._current = v;
                return v != QueryMatch.Invalid;
            }

            return new BinaryMatch<TInner, TOuter>(in inner, in outer, &SeekToFunc, &MoveNextFunc, inner.Count + outer.Count);
        }
    }
}
