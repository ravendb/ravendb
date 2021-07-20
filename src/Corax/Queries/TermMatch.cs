using System.Runtime.CompilerServices;
using Sparrow.Server.Compression;
using Voron.Data.Sets;
using Voron.Data.Containers;
using System;
using System.Diagnostics;

namespace Corax.Queries
{
    public unsafe struct TermMatch : IQueryMatch
    {
        private readonly delegate*<ref TermMatch, Span<long>, int> _fillFunc;
        private readonly delegate*<ref TermMatch, Span<long>, int> _andWithFunc;

        private readonly long _totalResults;
        private long _currentIdx;
        private long _baselineIdx;
        private long _current;

        private Container.Item _container;
        private Set.Iterator _set;

        public long Count => _totalResults;

        private TermMatch(
            delegate*<ref TermMatch, Span<long>, int> fillFunc,
            delegate*<ref TermMatch, Span<long>, int> andWithFunc,
            long totalResults)
        {
            _totalResults = totalResults;
            _current = QueryMatch.Start;
            _currentIdx = QueryMatch.Start;
            _baselineIdx = QueryMatch.Start;
            _fillFunc = fillFunc;
            _andWithFunc = andWithFunc;

            _container = default;
            _set = default;
        }

        public static TermMatch CreateEmpty()
        {
            static int FillFunc(ref TermMatch term, Span<long> matches)
            {
                term._currentIdx = QueryMatch.Invalid;
                term._current = QueryMatch.Invalid;
                return 0;
            }

            return new TermMatch(&FillFunc, &FillFunc, 0);
        }

        public static TermMatch YieldOnce(long value)
        {
            static int FillFunc(ref TermMatch term, Span<long> matches)
            {
                if (term._currentIdx == QueryMatch.Start)
                {
                    term._currentIdx = term._current;
                    matches[0] = term._current;
                    return 1;
                }

                term._currentIdx = QueryMatch.Invalid;
                return 0;
            }

            static int AndWithFunc(ref TermMatch term, Span<long> matches)
            {
                // TODO: If matches is too big, we should use quicksort
                long current = term._current;
                for (int i = 0; i < matches.Length; i++)
                {
                    if (matches[i] == current)
                    {
                        matches[0] = current;
                        return 1;
                    }
                }
                return 0;
            }


            return new TermMatch(&FillFunc, &AndWithFunc, 1)
            {
                _current = value,
                _currentIdx = QueryMatch.Start
            };
        }

        public static TermMatch YieldSmall(Container.Item containerItem)
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int FillFunc(ref TermMatch term, Span<long> matches)
            {
                // Fill needs to store resume capability.

                var stream = term._container.ToSpan();
                if (term._currentIdx == QueryMatch.Invalid || term._currentIdx >= stream.Length)
                {
                    term._currentIdx = QueryMatch.Invalid;
                    return 0;
                }

                int i = 0;
                for (; i < matches.Length; i++)
                {
                    term._current += ZigZagEncoding.Decode<long>(stream, out var len, (int)term._currentIdx);
                    term._currentIdx += len;
                    matches[i] = term._current;

                    if (term._currentIdx >= stream.Length)
                    {
                        i++;
                        break;
                    }                        
                }

                return i;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int AndWithFunc(ref TermMatch term, Span<long> matches)
            {
                // AndWith has to start from the start.
                // TODO: Support Seek for the small set in order to have better behavior.

                var stream = term._container.ToSpan();

                // need to seek from start
                long current = 0;
                int currentIdx = (int)term._baselineIdx;

                int i = 0;
                int matchedIdx = 0;
                while (currentIdx < stream.Length && i < matches.Length)
                {
                    current += ZigZagEncoding.Decode<long>(stream, out var len, currentIdx);
                    currentIdx += len;

                    while (matches[i] < current)
                    {                        
                        i++;
                        if (i >= matches.Length)
                            goto End;
                    }                    

                    // If there is a match we advance. 
                    if (matches[i] == current)
                    {
                        matches[matchedIdx++] = current;
                        i++;
                    }
                }

                End:  return matchedIdx;
            }

            var itemsCount = ZigZagEncoding.Decode<int>(containerItem.ToSpan(), out var len);
            return new TermMatch(&FillFunc, &AndWithFunc, itemsCount)
            {
                _container = containerItem,
                _currentIdx = len,
                _baselineIdx = len,
                _current = 0
            };
        }

        public static TermMatch YieldSet(Set set)
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int AndWithFunc(ref TermMatch term, Span<long> matches)
            {
                int matchedIdx = 0;

                term._set.Seek(matches[0] - 1);

                // We update the current value we want to work with.
                var current = term._set.Current;

                // Check if there are matches left to process or is any posibility of a match to be available in this block.
                int i = 0;                
                while (i < matches.Length && current <= matches[^1])
                {
                    // While the current match is smaller we advance.
                    while (matches[i] < current)
                    {
                        i++;
                        if (i >= matches.Length)
                            goto End;
                    }

                    // We are guaranteed that matches[i] is at least equal if not higher than current.
                    Debug.Assert(matches[i] >= current);

                    // We have a match, we include it into the matches and go on. 
                    if (current == matches[i])
                    {
                        matches[matchedIdx++] = current;
                        i++;
                    }

                    // We look into the next.
                    if (term._set.MoveNext() == false)
                        goto End;

                    current = term._set.Current;                    
                }

                End:
                term._current = current;
                return matchedIdx;
            }


            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            static int FillFunc(ref TermMatch term, Span<long> matches)
            {
                int i = 0;
                while (i < matches.Length && term._set.MoveNext())
                {
                    matches[i++] = term._set.Current;
                }
                return i;
            }


            return new TermMatch(&FillFunc, &AndWithFunc, set.State.NumberOfEntries)
            {
                _set = set.Iterate(),
                _current = long.MinValue
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> matches)
        {
            return _fillFunc(ref this, matches);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> matches)
        {
            return _andWithFunc(ref this, matches);
        }
    }
}
