using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Server;
using Size = Voron.Global.Constants.Size;

namespace Corax.Queries
{
    [DebuggerDisplay("{DebugView,nq}")]
    public unsafe struct MultiTermBoostingMatch<TTermProvider> : IQueryMatch
        where TTermProvider : ITermProvider
    {
        internal long _totalResults;
        internal long _current;
        internal long _currentIdx;        
        private QueryCountConfidence _confidence;
        private readonly IQueryScoreFunction _scoring;

        internal TTermProvider _inner;
        private TermMatch _currentTerm;
        private readonly ByteStringContext _context;

        internal long* _documentBuffer;
        internal long* _countBuffer;
        internal bool _requiresSort;

        internal int _bufferSize;
        internal int _bufferIdx;        
        internal IDisposable _bufferHandler;
        private readonly delegate*<ref MultiTermBoostingMatch<TTermProvider>, Span<long>, Span<float>, void> _scoreFunc;
        private readonly delegate*<ref MultiTermBoostingMatch<TTermProvider>, QueryInspectionNode> _inspectFunc;

        public bool IsBoosting => true;
        public long Count => _totalResults;
        public long Current => _currentIdx <= QueryMatch.Start ? _currentIdx : _current;

        public QueryCountConfidence Confidence => _confidence;

        public MultiTermBoostingMatch(
            ByteStringContext context, TTermProvider inner,
            delegate*<ref MultiTermBoostingMatch<TTermProvider>, QueryInspectionNode> inspectFunc,
            long totalResults = 0, QueryCountConfidence confidence = QueryCountConfidence.Low,
            IQueryScoreFunction scoring = null,
            delegate*<ref MultiTermBoostingMatch<TTermProvider>, Span<long>, Span<float>, void> scoreFunc = null)
        {
            _inner = inner;
            _context = context;
            _current = QueryMatch.Start;
            _currentIdx = QueryMatch.Start;
            _totalResults = totalResults;
            _confidence = confidence;
            _scoreFunc = scoreFunc;
            _inspectFunc = inspectFunc;
            _scoring = scoring;

            _inner.Next(out _currentTerm);

            int bufferSize = 4 * Math.Max(Size.Kilobyte, (int)_currentTerm.Count);
            _bufferHandler = _context.Allocate(2 * bufferSize * sizeof(long), out var buffer);
            _bufferSize = bufferSize;
            _documentBuffer = (long*)buffer.Ptr;
            _countBuffer = (long*)buffer.Ptr + bufferSize;
            _bufferIdx = 0;
            _requiresSort = false;
        }

        [SkipLocalsInit]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> buffer)
        {
            if (_current == QueryMatch.Invalid)
                return 0;

            int count = 0;
            var bufferState = buffer;
            bool requiresSort = false;

            long docsCount = _currentTerm.Count;
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
                    docsCount = _currentTerm.Count;
                    requiresSort |= count != buffer.Length;

                    // This is signaling the requirement to sort the scores arrays.
                    _requiresSort = true;
                }
                
                // Every time this is called we will store in a growable temporary buffer all the matches for this boosting to be used later.
                var docSlice = new Span<long>(_documentBuffer + _bufferIdx, _bufferSize - _bufferIdx);
                if (docSlice.Length < read)
                {
                    UnlikelyGrowBuffer(docSlice.Length + read);
                    docSlice = new Span<long>(_documentBuffer + _bufferIdx, _bufferSize - _bufferIdx);
                }
                var countSlice = new Span<long>(_countBuffer + _bufferIdx, _bufferSize - _bufferIdx);
                
                bufferState[..read].CopyTo(docSlice); // Copy the documents to the retain buffer
                countSlice[..read].Fill(docsCount);  // Fill the counts array with the count for that particular document.

                bufferState = bufferState.Slice(read);
                count += read;
                _bufferIdx += read;
            }

            _current = count != 0 ? buffer[count - 1] : QueryMatch.Invalid;

        End:
        if (requiresSort && count > 1)
        {
            // We need to sort and remove duplicates.
            fixed (long* bufferBasePtr = buffer)
            {

                MemoryExtensions.Sort(buffer.Slice(0, count));

                // We need to fill in the gaps left by removing deduplication process.
                // If there are no duplicated the writes at the architecture level will execute
                // way faster than if there are.

                var outputBufferPtr = bufferBasePtr;

                var bufferPtr = bufferBasePtr;
                var bufferEndPtr = bufferBasePtr + count - 1;
                while (bufferPtr < bufferEndPtr)
                {
                    outputBufferPtr += bufferPtr[1] != bufferPtr[0] ? 1 : 0;
                    *outputBufferPtr = bufferPtr[1];

                    bufferPtr++;
                }

                count = (int)(outputBufferPtr - bufferBasePtr + 1);
            }
        }

        return count;
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
            var bufferHandler = _context.Allocate(2 * size, out var buffer);

            // Ensure we copy the content and then switch the buffers. 

            new Span<long>(_documentBuffer, _bufferIdx).CopyTo(new Span<long>(buffer.Ptr, size));
            new Span<long>(_countBuffer, _bufferIdx).CopyTo(new Span<long>(buffer.Ptr + currentLength, size));
            _bufferHandler.Dispose();

            _bufferSize = size;
            _documentBuffer = (long*)buffer.Ptr;
            _countBuffer = (long*)buffer.Ptr + size;            
            _bufferHandler = bufferHandler;
        }

        [SkipLocalsInit]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer, int matches)
        {
            throw new NotSupportedException($"{nameof(AndWith)} is not supported for {nameof(MultiTermBoostingMatch<TTermProvider>)}");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Score(Span<long> matches, Span<float> scores)
        {
            if (_scoreFunc == null)
                return; // We ignore. Nothing to do here.             

            if (_requiresSort)
            {
                // We know we can get unordered sequences. 
                Sorter<long, long, NumericComparer> sorter;

                long* documentPtr = _documentBuffer;
                long* countPtr = _countBuffer;
                int bufferCount = _bufferIdx;

                // We will sort the documents and the counts before performing deduplication and keeping the lowest count
                // The reason is because the rarer the document the highest its score. 
                var documentsSpan = new Span<long>(documentPtr, bufferCount);
                var countSpan = new Span<long>(countPtr, bufferCount);
                sorter.Sort(documentsSpan, countSpan);

                // PERF: If there are no duplicated the writes at the architecture level will execute
                // way faster than if there are.
                var outputDocumentIdx = 0;
                var countBasePtr = countPtr;
                var documentBasePtr = documentPtr;                                                
                var documentEndPtr = documentPtr + bufferCount - 1;
                while (documentPtr < documentEndPtr)
                {
                    bool different = documentPtr[1] != documentPtr[0];
                    outputDocumentIdx += different ? 1 : 0;

                    documentBasePtr[outputDocumentIdx] = documentPtr[1];
                    countBasePtr[outputDocumentIdx] = Math.Min(countPtr[1], countBasePtr[outputDocumentIdx]);

                    documentPtr++;
                    countPtr++;
                }

                _requiresSort = false;
            }

            _scoreFunc(ref this, matches, scores);
        }

        public QueryInspectionNode Inspect()
        {
            return _inspectFunc is null ? QueryInspectionNode.NotInitializedInspectionNode(nameof(MultiTermBoostingMatch<TTermProvider>)) : _inspectFunc(ref this);
        }

        string DebugView => Inspect().ToString();

        public static MultiTermBoostingMatch<TTermProvider> Create<TScoreFunction>(IndexSearcher searcher, TTermProvider inTermProvider, TScoreFunction scoreFunction) 
            where TScoreFunction : IQueryScoreFunction
        {
            static QueryInspectionNode InspectFunc(ref MultiTermBoostingMatch<TTermProvider> match)
            {
                return new QueryInspectionNode($"{nameof(MultiTermBoostingMatch<TTermProvider>)} [{typeof(TScoreFunction).Name}]",
                    children: new List<QueryInspectionNode> { match._inner.Inspect() },
                    parameters: new Dictionary<string, string>()
                    {
                        { nameof(match.IsBoosting), match.IsBoosting.ToString() },
                        { nameof(match.Count), $"{match.Count} [{match.Confidence}]" }
                    });
            }

            if (typeof(TScoreFunction) == typeof(ConstantScoreFunction))
            {
                return new MultiTermBoostingMatch<TTermProvider>(searcher.Allocator, inTermProvider, &InspectFunc, scoring: scoreFunction, scoreFunc: &ConstantScoreFunc);
            }
            else if (typeof(TScoreFunction) == typeof(TermFrequencyScoreFunction))
            {
                return new MultiTermBoostingMatch<TTermProvider>(searcher.Allocator, inTermProvider, &InspectFunc, scoring: scoreFunction, scoreFunc: &TermFrequencyScoreFunc);
            }
            else throw new NotSupportedException($"The scoring function '{typeof(TScoreFunction).Name}' is not supported.");
        }

        private static void TermFrequencyScoreFunc(ref MultiTermBoostingMatch<TTermProvider> match, Span<long> matches, Span<float> scores)
        {
            int bufferCount = match._bufferIdx;
            var documentsSpan = new Span<long>(match._documentBuffer, bufferCount);
            var countSpan = new Span<long>(match._countBuffer, bufferCount);

            // Get rid of all the elements that are smaller than the first one.
            int eIdx = 0;
            long matchValue = matches[0];
            while (eIdx < documentsSpan.Length && matchValue > documentsSpan[eIdx])
                eIdx++;

            int mIdx = 0;
            // We know it is not boosting, so we assign the constant instead.
            while (eIdx < documentsSpan.Length)
            {
                long elementValue = documentsSpan[eIdx];
                if (elementValue < matchValue)
                {
                    // We are gonna try to iterate over this as fast as possible.
                    eIdx++;
                    continue;
                }
                else if (elementValue == matchValue)
                {
                    // When we find a match we apply the score.
                    float termCount = 1.0f / countSpan[eIdx];
                    scores[mIdx] = termCount;
                    eIdx++;
                }

                mIdx++;
                if (mIdx >= matches.Length)
                    break;

                matchValue = matches[mIdx];
            }
        }

        internal static void ConstantScoreFunc(ref MultiTermBoostingMatch<TTermProvider> match, Span<long> matches, Span<float> scores)
        {
            int bufferCount = match._bufferIdx;
            var documentsSpan = new Span<long>(match._documentBuffer, bufferCount);

            // Get rid of all the elements that are smaller than the first one.
            int eIdx = 0;
            long matchValue = matches[0];
            while (eIdx < documentsSpan.Length && matchValue > documentsSpan[eIdx])
                eIdx++;

            var score = ((ConstantScoreFunction)match._scoring).Value;

            int mIdx = 0;
            // We know it is not boosting, so we assign the constant instead.
            while (eIdx < documentsSpan.Length)
            {
                long elementValue = documentsSpan[eIdx];
                if (elementValue < matchValue)
                {
                    // We are gonna try to iterate over this as fast as possible.
                    eIdx++;
                    continue;
                }
                else if (elementValue == matchValue)
                {
                    // When we find a match we apply the score.
                    scores[mIdx] = score;
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
