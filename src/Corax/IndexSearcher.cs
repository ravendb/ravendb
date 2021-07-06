using System;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using Sparrow.Server.Compression;
using Voron;
using Voron.Impl;
using Voron.Data.Sets;
using Voron.Data.Containers;

namespace Corax
{
    public interface IIndexMatch
    {
        long TotalResults { get; }
        long Current { get; }

        bool SeekTo(long next = 0);
        bool MoveNext(out long v);
    }

    public static class QueryMatch
    {
        public const long Invalid = -1;
    }

    public unsafe struct TermMatch : IIndexMatch
    {
        private readonly delegate*<ref TermMatch, long, bool> _seekToFunc;
        private readonly delegate*<ref TermMatch, out long, bool> _moveNext;

        private long _totalResults;
        private long _currentIdx;
        private long _current;
        
        private Container.Item _container;
        private Set.Iterator _set;

        public long TotalResults => _totalResults;
        public long Current => _currentIdx == QueryMatch.Invalid ? QueryMatch.Invalid : _current;

        private TermMatch(delegate*<ref TermMatch, long, bool> seekFunc, delegate*<ref TermMatch, out long, bool> moveNext, long totalResults)
        {
            _totalResults = totalResults;
            _current = QueryMatch.Invalid;
            _currentIdx = QueryMatch.Invalid;
            _seekToFunc = seekFunc;
            _moveNext = moveNext;

            _container = default;
            _set = default;
        }

        public static TermMatch CreateEmpty()
        {
            static bool SeekFunc(ref TermMatch term, long next)
            {
                term._current = QueryMatch.Invalid;
                return false;
            }

            static bool MoveNextFunc(ref TermMatch term, out long v)
            {
                Unsafe.SkipInit(out v);
                return false;
            }

            return new TermMatch(&SeekFunc, &MoveNextFunc, 0);
        }

        public static TermMatch YieldOnce(long value)
        {
            static bool SeekFunc(ref TermMatch term, long next)
            {
                term._currentIdx = next > term._current ? QueryMatch.Invalid : 0;
                return term._currentIdx == 0;
            }

            static bool MoveNextFunc(ref TermMatch term, out long v)
            {
                Unsafe.SkipInit(out v);
                term._currentIdx = QueryMatch.Invalid;
                return false;
            }

            return new TermMatch(&SeekFunc, &MoveNextFunc, 1)
            {
                _current = value
            };
        }

        public static TermMatch YieldSmall(Container.Item containerItem)
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static bool SeekFunc(ref TermMatch term, long next)
            {
                var stream = term._container.ToSpan();

                int pos = 0;
                long current = QueryMatch.Invalid;
                while (pos < stream.Length)
                {
                    current = ZigZagEncoding.Decode<long>(stream, out var len, pos);
                    pos += len;
                    if (current > next)
                    {
                        // We found values bigger than next.
                        term._current = current;
                        term._currentIdx = pos;
                        return true;
                    }
                }

                term._current = QueryMatch.Invalid;
                term._currentIdx = QueryMatch.Invalid;
                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static bool MoveNextFunc(ref TermMatch term, out long v)
            {
                var stream = term._container.ToSpan();
                if (term._currentIdx == QueryMatch.Invalid || term._currentIdx >= stream.Length)
                {
                    Unsafe.SkipInit(out v);
                    return false;
                }

                v = ZigZagEncoding.Decode<long>(stream, out var len, (int)term._currentIdx);
                term._current = v;
                term._currentIdx += len;

                return true;
            }

            var itemsCount = ZigZagEncoding.Decode<int>(containerItem.ToSpan(), out var len);
            return new TermMatch(&SeekFunc, &MoveNextFunc, itemsCount)
            {
                _container = containerItem,
                _currentIdx = len
            };
        }

        public static TermMatch YieldSet(Set set)
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static bool SeekFunc(ref TermMatch term, long next)
            {
                return term._set.Seek(next);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static bool MoveNextFunc(ref TermMatch term, out long v)
            {
                bool hasMove = term._set.MoveNext();
                v = hasMove ? term._set.Current : QueryMatch.Invalid;
                term._current = v;
                return hasMove;
            }

            return new TermMatch(&SeekFunc, &MoveNextFunc, set.State.NumberOfEntries)
            {                
                _set = set.Iterate()
            };
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
    }

    public unsafe struct BinaryMatch : IIndexMatch
    {
        private readonly delegate*<ref BinaryMatch, long, bool> _seekToFunc;
        private readonly delegate*<ref BinaryMatch, out long, bool> _moveNext;

        private readonly IIndexMatch _inner;
        private readonly IIndexMatch _outer;

        private long _totalResults;
        private long _current;

        public long TotalResults => _totalResults;
        public long Current => _current;

        private BinaryMatch(delegate*<ref BinaryMatch, long, bool> seekFunc, delegate*<ref BinaryMatch, out long, bool> moveNext, long totalResults)
        {
            _totalResults = totalResults;
            _current = QueryMatch.Invalid;
            _seekToFunc = seekFunc;
            _moveNext = moveNext;
            _inner = default;
            _outer = default;
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
        public static BinaryMatch YieldAnd<TInner, TOuter>(ref TInner inner, ref TOuter outer)
            where TInner : struct, IIndexMatch
            where TOuter : struct, IIndexMatch
        {
            static bool SeekToFunc(ref BinaryMatch match, long v)
            {
                var inner = (TInner)match._inner;
                var outer = (TOuter)match._outer;

                return inner.SeekTo(v) && outer.SeekTo(v);
            }

            static bool MoveNextFunc(ref BinaryMatch match, out long v)
            {
                var inner = (TInner)match._inner;
                var outer = (TOuter)match._outer;

                Unsafe.SkipInit(out v);
                if (inner.Current == QueryMatch.Invalid || outer.Current == QueryMatch.Invalid)
                    return false;

                while (inner.Current != outer.Current)
                {
                    if (inner.Current < outer.Current)
                    {
                        if (inner.MoveNext(out v) == false)
                            return false;
                    }
                    else
                    {
                        if (outer.MoveNext(out v) == false)
                            return false;
                    }
                }
                return inner.Current == outer.Current;
            }

            return new BinaryMatch(&SeekToFunc, &MoveNextFunc, Math.Min(inner.TotalResults, outer.TotalResults));
        }

        public static BinaryMatch YieldOr<TInner, TOuter>(ref TInner inner, ref TOuter outer)
            where TInner : struct, IIndexMatch
            where TOuter : struct, IIndexMatch
        {
            throw new NotImplementedException();
        }
    }

    public class IndexSearcher : IDisposable
    {
        private readonly StorageEnvironment _environment;
        private readonly Transaction _transaction;

        // The reason why we want to have the transaction open for us is so that we avoid having
        // to explicitly provide the index searcher with opening semantics and also every new
        // searcher becomes essentially a unit of work which makes reusing assets tracking more explicit.
        public IndexSearcher(StorageEnvironment environment)
        {
            _environment = environment;
            _transaction = environment.ReadTransaction();
        }

        // foreach term in 2010 .. 2020
        //     yield return TermMatch(field, term)// <-- one term , not sorted

        // userid = UID and date between 2010 and 2020 <-- 100 million terms here 
        // foo = bar and published = true

        // foo = bar
        public TermMatch TermQuery(string field, string term)
        {
            var fields = _transaction.ReadTree(IndexWriter.FieldsSlice);
            var terms = fields.CompactTreeFor(field);
            if (terms == null || terms.TryGetValue(term, out var value) == false)
                return TermMatch.CreateEmpty();
            
            TermMatch matches;
            if ((value & (long)TermIdMask.Set) != 0)
            {
                var setId = value & ~0b11;
                var setStateSpan = Container.Get(_transaction.LowLevelTransaction, setId).ToSpan();
                ref readonly var setState = ref MemoryMarshal.AsRef<SetState>(setStateSpan);
                var set = new Set(_transaction.LowLevelTransaction, Slices.Empty, setState);
                matches = TermMatch.YieldSet(set);                
            }
            else if ((value & (long)TermIdMask.Single) != 0)
            {
                var smallSetId = value & ~0b11;
                var small = Container.Get(_transaction.LowLevelTransaction, smallSetId);
                matches = TermMatch.YieldSmall(small);                
            }
            else
            {
                matches = TermMatch.YieldOnce(value);
            }
                
            matches.SeekTo(0);
            return matches;
        }


        //public BinaryMatch And(in TermMatch set1, in TermMatch set2)
        //{
        //    return BinaryMatch.YieldAnd(set1, set2);
        //}

        //public BinaryMatch Or(in TermMatch set1, in TermMatch set2)
        //{
        //    return BinaryMatch.YieldOr(set1, set2);
        //}

        public void Dispose()
        {
            _transaction?.Dispose();
        }
    }
}
