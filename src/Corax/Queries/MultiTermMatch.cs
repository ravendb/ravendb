using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace Corax.Queries
{
    public unsafe struct MultiTermMatch<TTermProvider> : IQueryMatch
        where TTermProvider : ITermProvider
    {
        internal long _totalResults;
        internal long _current;
        internal long _currentIdx;
        private QueryCountConfidence _confidence;
        
        internal TTermProvider _inner;
        private TermMatch _currentTerm;        

        public long Count => _totalResults;
        public long Current => _currentIdx <= QueryMatch.Start ? _currentIdx : _current;

        public QueryCountConfidence Confidence => throw new NotImplementedException();

        public MultiTermMatch(TTermProvider inner, long totalResults = 0, QueryCountConfidence confidence = QueryCountConfidence.Low)
        {
            _inner = inner;            
            _current = QueryMatch.Start;
            _currentIdx = QueryMatch.Start;
            _totalResults = totalResults;
            _confidence = confidence;

            _inner.Next(out _currentTerm);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> buffer)
        {
            if (_current == QueryMatch.Invalid)
                return 0;

            int count = 0;
            var bufferState = buffer;
            bool requiresSort = false;
            while (bufferState.Length > 0)
            {
                var read = _currentTerm.Fill(bufferState);                
                if (read == 0)
                {
                    if (_inner.Next(out _currentTerm) == false)
                    {
                        _current = QueryMatch.Invalid;
                        goto End;                        
                    }
                    
                    // We can prove that we need sorting and deduplication in the end. 
                    requiresSort |= count != buffer.Length;
                }
                
                count += read;
                bufferState = bufferState.Slice(read);
            }

            _current = count != 0 ? buffer[count - 1] : QueryMatch.Invalid;

            End:
            if (requiresSort && count > 1)
            {
                // We need to sort and remove duplicates.
                bufferState = buffer.Slice(0, count);             
                MemoryExtensions.Sort(bufferState);

                // We need to fill in the gaps left by removing deduplication process.
                // If there are no duplicated the writes at the architecture level will execute
                // way faster than if there are.
                count = 0;
                for (int i = 1; i < bufferState.Length; i++)
                {
                    count += bufferState[i] != bufferState[i - 1] ? 1 : 0;
                    bufferState[count] = bufferState[i];
                }
                bufferState[count++] = bufferState[^1];
            }
            
            return count;
        }

        [SkipLocalsInit]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer)
        {
            // TODO: We should consider the policy where it makes sense to actually implement something different to avoid
            //       the N^2 check here. While could be interesting to do now we still dont know what are the conditions
            //       that would trigger such optimization or if they are even necessary. The reason why is that buffer size
            //       may offset some of the requirements for such a scan operation. Interesting approaches to consider include
            //       evaluating directly, construct temporary data structures like bloom filters on subsequent iterations when
            //       the statistics guarrant those approaches, etc.

            Span<long> tmp = stackalloc long[buffer.Length];
            Span<long> tmp2 = stackalloc long[buffer.Length];
            Span<long> results = stackalloc long[buffer.Length];

            _inner.Reset();
            int totalSize = 0;
            while (totalSize < buffer.Length && _inner.Next(out var current))
            {
                buffer.CopyTo(tmp);
                var read = current.AndWith(tmp);
                if (read == 0)
                    continue;

                results.Slice(0, totalSize).CopyTo(tmp2);
                totalSize = MergeHelper.Or(results, tmp2.Slice(0, totalSize), tmp.Slice(0, read));
            }
            results.Slice(0, totalSize).CopyTo(buffer);
            return totalSize;
        }
    }
}
