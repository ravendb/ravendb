using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Utils;
using Sparrow;
using Sparrow.Server;
using Size = Voron.Global.Constants.Size;

namespace Corax.Queries
{
    public struct BoostingComparer : IMatchComparer
    {
        public MatchCompareFieldType FieldType => MatchCompareFieldType.Score;

        public FieldMetadata Field => throw new NotSupportedException($"{nameof(Field)} is not supported for {nameof(BoostingComparer)}");

        public int CompareById(long idx, long idy)
        {
            throw new NotSupportedException($"{nameof(CompareById)} is not supported for {nameof(BoostingComparer)}");
        }
        
        public int CompareNumerical<T>(T sx, T sy) where T : unmanaged
        { 
            throw new NotSupportedException($"{nameof(CompareNumerical)} is not supported for {nameof(BoostingComparer)}");
        }

        public int CompareSequence(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
        {
            throw new NotSupportedException($"{nameof(CompareSequence)} is not supported for {nameof(BoostingComparer)}");
        }
    }

    [DebuggerDisplay("{DebugView,nq}")]
    public unsafe struct BoostingMatch<TInner, TQueryScoreFunction> : IQueryMatch
        where TInner : IQueryMatch
        where TQueryScoreFunction : IQueryScoreFunction
    {
        internal TInner _inner;
        internal TQueryScoreFunction _scorer;
        internal readonly IndexSearcher _searcher;
        internal long* _buffer;
        internal int _bufferSize;
        internal int _bufferIdx;
        internal int _countOfCalls;
        internal bool _requiresSort;
        internal IDisposable _bufferHandler;

        private readonly delegate*<ref BoostingMatch<TInner, TQueryScoreFunction>, Span<long>, Span<float>, void> _scoreFunc;
        private readonly delegate*<ref BoostingMatch<TInner, TQueryScoreFunction>, QueryInspectionNode> _inspectFunc;

        public BoostingMatch(in IndexSearcher searcher, in TInner inner, 
            in TQueryScoreFunction scorer, delegate*<ref BoostingMatch<TInner, TQueryScoreFunction>, Span<long>, Span<float>, void> scoreFunc,
            delegate*<ref BoostingMatch<TInner, TQueryScoreFunction>, QueryInspectionNode> inspectFunc)
        {
            _inner = inner;
            _scorer = scorer;
            _scoreFunc = scoreFunc;
            _inspectFunc = inspectFunc;

            _searcher = searcher;
            _bufferIdx = 0;
            _countOfCalls = 0;
            _requiresSort = false;

            ByteString buffer;
            int bufferSize;
            if (inner.Confidence == QueryCountConfidence.High)
            {
                bufferSize = (int)inner.Count;
            }
            else if (inner.Confidence == QueryCountConfidence.Normal)
            {
                bufferSize = 2 * (int)inner.Count;
            }
            else
            {
                bufferSize = 4 * Size.Kilobyte;
            }

            _bufferHandler = searcher.Allocator.Allocate(bufferSize * sizeof(long) + bufferSize, out buffer);
            _bufferSize = bufferSize;
            _buffer = (long*)buffer.Ptr;
        }

        public long Count => _inner.Count;

        public QueryCountConfidence Confidence => _inner.Confidence;

        public bool IsBoosting => true;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer, int matches)
        {            
            var results = _inner.AndWith(buffer, matches);
            if (results == 0)
                return results;

            // Every time this is called we will store in a growable temporary buffer all the matches for this boosting to be used later.
            var bufferSlice = new Span<long>(_buffer + _bufferIdx, _bufferSize - _bufferIdx);
            if (bufferSlice.Length < results)
            {
                UnlikelyGrowBuffer(bufferSlice.Length + results);
                bufferSlice = new Span<long>(_buffer + _bufferIdx, _bufferSize - _bufferIdx);                
            }

            buffer.Slice(0, results).CopyTo(bufferSlice);
            _bufferIdx += results;

            // We track the amount of times this is called in order to know if we need to sort the buffer when scoring.             
            _countOfCalls++;
            if (_countOfCalls > 1)
                _requiresSort = true;

            return results;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> matches)
        {
            int results = _inner.Fill(matches);
            if (results == 0)
                return results;

            // Every time this is called we will store in a growable temporary buffer all the matches for this boosting to be used later.
            var bufferSlice = new Span<long>(_buffer + _bufferIdx, _bufferSize - _bufferIdx);
            if (bufferSlice.Length < results)
            {
                UnlikelyGrowBuffer(bufferSlice.Length + results);
                bufferSlice = new Span<long>(_buffer + _bufferIdx, _bufferSize - _bufferIdx);
            }

            var frequencyBuffer = new Span<byte>(_buffer + _bufferSize + _bufferIdx, _bufferSize - _bufferIdx);
         //   (FrequencyUtils.DecodeWithFrequenciesIntoBuffersAndDiscard(Span<long> source,)
            matches.Slice(0, results).CopyTo(bufferSlice);

            _bufferIdx += results;

            // We track the amount of times this is called in order to know if we need to sort the buffer when scoring.             
            _countOfCalls++;
            if (_countOfCalls > 1)
                _requiresSort = true;

            return results;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void UnlikelyGrowBuffer(int size)
        {
            // Calculate the new size. 
            int currentLength = _bufferSize;
            if (currentLength > 16 * Size.Megabyte)
            {
                size = (int)(size * 1.5);
            }
            else
            {
                size = (int)Math.Max(currentLength * 2, size * 1.2);
            }

            // Allocate the new buffer
            var bufferHandler = _searcher.Allocator.Allocate(size * sizeof(long), out var buffer);
            
            // Ensure we copy the content and then switch the buffers. 
            new Span<long>(_buffer, _bufferIdx).CopyTo(new Span<long>(buffer.Ptr, size));

            _bufferHandler.Dispose();

            _buffer = (long*)buffer.Ptr;
            _bufferSize = size;
            _bufferHandler = bufferHandler;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Score(Span<long> matches, Span<float> scores)
        {
            if (_inner.IsBoosting)
            {
                // Inner is also boosting, so we pass it along to ensure we get the proper scores before applying the current ones.
                _inner.Score(matches, scores);
            }   

            if (_requiresSort)
            {
                // We know we can get unordered sequences. 
                Sorter<long, NumericComparer> sorter;
                sorter.Sort(new Span<long>(_buffer, _bufferIdx));

                _requiresSort = false;
            }

            _scoreFunc(ref this, matches, scores);
        }

        public QueryInspectionNode Inspect()
        {
            return _inspectFunc is null ? QueryInspectionNode.NotInitializedInspectionNode(nameof(BoostingMatch)) : _inspectFunc(ref this);
        }

        string DebugView => Inspect().ToString();

        internal static void ConstantScoreFunc(ref BoostingMatch<TInner, ConstantScoreFunction> match, Span<long> matches, Span<float> scores)
        {
            // We need to multiply by a constant.                       
            float constValue = match._scorer.Value;

            Span<long> elements = new Span<long>(match._buffer, match._bufferIdx);

            // Get rid of all the elements that are smaller than the first one.
            int eIdx = 0;
            long matchValue = matches[0];
            while (eIdx < elements.Length && matchValue > elements[eIdx])
                eIdx++;

            int mIdx = 0;
            if (match._inner.IsBoosting)
            {
                while (eIdx < elements.Length)
                {
                    long elementValue = elements[eIdx];
                    if (elementValue < matchValue)
                    {
                        // We are gonna try to iterate over this as fast as possible.
                        eIdx++;
                        continue;
                    }
                    else if (elementValue == matchValue)
                    {
                        // When we find a match we apply the score.
                        scores[mIdx] *= constValue;
                        eIdx++;
                    }

                    mIdx++;
                    if (mIdx >= matches.Length)
                        break;
                    matchValue = matches[mIdx];
                }
            }
            else
            {
                // We know it is not boosting, so we assign the constant instead.
                while (eIdx < elements.Length)
                {
                    long elementValue = elements[eIdx];
                    if (elementValue < matchValue)
                    {
                        // We are gonna try to iterate over this as fast as possible.
                        eIdx++;
                        continue;
                    }
                    else if (elementValue == matchValue)
                    {
                        // When we find a match we apply the score.
                        scores[mIdx] = constValue;
                        eIdx++;
                    }

                    mIdx++;
                    if (mIdx >= matches.Length)
                        break;
                    matchValue = matches[mIdx];
                }
            }
        }
        
        internal static void TermFrequencyScoreFunc(ref BoostingMatch<TInner, TermFrequencyScoreFunction> match, Span<long> matches, Span<float> scores)
        {
            if (typeof(TInner) != typeof(TermMatch))
                throw new NotSupportedException($"The type {nameof(TInner)} is not supported for calculating Term Frequency Score");
                      
            Span<long> elements = new Span<long>(match._buffer, match._bufferIdx);

            // Get rid of all the elements that are smaller than the first one.
            int eIdx = 0;
            long matchValue = matches[0];
            while (eIdx < elements.Length && matchValue > elements[eIdx])
                eIdx++;

            int mIdx = 0;
            float termCount = 1.0f / match._inner.Count;
            if (match._inner.IsBoosting)
            {
                while (eIdx < elements.Length)
                {
                    long elementValue = elements[eIdx];
                    if (elementValue < matchValue)
                    {
                        // We are gonna try to iterate over this as fast as possible.
                        eIdx++;
                        continue;
                    }
                    else if (elementValue == matchValue)
                    {
                        // When we find a match we apply the score.
                        scores[mIdx] *= termCount;
                        eIdx++;
                    }

                    mIdx++;
                    if (mIdx >= matches.Length)
                        break;
                    matchValue = matches[mIdx];
                }
            }
            else
            {                
                // We know it is not boosting, so we assign the constant instead.
                while (eIdx < elements.Length)
                {
                    long elementValue = elements[eIdx];
                    if (elementValue < matchValue)
                    {
                        // We are gonna try to iterate over this as fast as possible.
                        eIdx++;
                        continue;
                    }
                    else if (elementValue == matchValue)
                    {
                        // When we find a match we apply the score.
                        scores[mIdx] = termCount;
                        eIdx++;
                    }

                    mIdx++;
                    if (mIdx >= matches.Length)
                        break;
                    matchValue = matches[mIdx];
                }
            }
        }
    }
}
