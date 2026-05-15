using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Querying.Matches.Meta;
using Sparrow;
using Sparrow.Server;
using Sparrow.Server.Utils;

namespace Corax.Querying.Matches
{
    [DebuggerDisplay("{DebugView,nq}")]
    public sealed class MemoizationMatchProvider<TInner> : IMemoizationMatchSource
             where TInner : IQueryMatch
    {
        private int _replayCounter;
        public int ReplayCounter => _replayCounter;

        private readonly ByteStringContext _ctx;
        private readonly Querying.IndexSearcher _indexSearcher;
        private TInner _inner;
        private bool _disposed;

        public bool IsBoosting => _inner.IsBoosting;
        public long Count => _inner.Count;
        public QueryCountConfidence Confidence => _inner.Confidence;

        private int _bufferEndIdx;
        private GrowableBuffer<long, Progressive<long>> _buffer;

        private SortMode _sortMode;

        private enum SortMode
        {
            Default,
            Required,
            Skip
        }

        public void SortingRequired()
        {
            _sortMode = SortMode.Required;
        }

        public void SkipSortingResults()
        {
            _sortMode = SortMode.Skip;
        }

        public MemoizationMatchProvider(IndexSearcher indexSearcher, in TInner inner)
        {
            _indexSearcher = indexSearcher;
            _ctx = indexSearcher.Allocator;
            _inner = inner;
            _replayCounter = 0;
            _buffer = new GrowableBuffer<long, Progressive<long>>();
            _bufferEndIdx = -1;
        }

        public MemoizationMatch Replay()
        {
            Debug.Assert(_disposed == false);
            _replayCounter++;
            return MemoizationMatch.Create(new MemoizationMatch<TInner>(this));
        }

        public Span<long> FillAndRetrieve()
        {
            Debug.Assert(_disposed == false);
            if (_bufferEndIdx < 0)
                InitializeInner();

            if (_bufferEndIdx == 0)
                return Span<long>.Empty;

            return _buffer.Results[.._bufferEndIdx];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int Fill(Span<long> matches) => _inner.Fill(matches);

        private void InitializeInner()
        {
            var maxItemsInBudget = _indexSearcher.MaxMemoizationSizeInBytes / sizeof(long);
            var initialSize = _inner.Confidence switch
            {
                QueryCountConfidence.High => _inner.Count,
                QueryCountConfidence.Normal => Math.Min(_inner.Count / 2, maxItemsInBudget),
                _ => 16,
            };
            
            if (_buffer.TryInit(_ctx, initialSize, _indexSearcher.MaxMemoizationSizeInBytes) == false)
                ThrowExceededMemoizationSize();

            while (true)
            {
                if (_buffer.TryGetSpace(out var space) == false)
                    ThrowExceededMemoizationSize();

                var read = _inner.Fill(space);
                if (read == 0)
                    break;

                _buffer.AddUsage(read);
            }

            // The problem is that multiple Fill calls do not ensure that we will get a sequence of ordered
            // values, therefore we must ensure that we get a 'sorted' sequence ensuring those happen.
            SetSortingMode();
            if (_sortMode == SortMode.Required)
            {
                // We need to sort and remove duplicates.
                _buffer.Truncate(Sorting.SortAndRemoveDuplicates(_buffer.Results));
            }

            _bufferEndIdx = _buffer.Count;

            void SetSortingMode()
            {
                var skipSorting = _inner.AttemptToSkipSorting();
                if (_sortMode == SortMode.Default)
                {
                    _sortMode = skipSorting switch
                    {
                        SkipSortingResult.ResultsNativelySorted => SortMode.Skip, // if the inner already sorted, we don't need
                        SkipSortingResult.WillSkipSorting => SortMode.Required, // if the inner skipped sorting, we have to
                        SkipSortingResult.SortingIsRequired => SortMode.Required,
                        _ => throw new ArgumentOutOfRangeException(skipSorting.ToString())
                    };
                }
            }

            void ThrowExceededMemoizationSize()
            {
                var inner = _inner.ToString();
                try
                {
                    inner = _inner.DebugView;
                }
                catch (Exception e)
                {
                    // we are protecting from an error in DebugView during error handling here
                    inner += " - DebugView failure " + e.Message;
                }

                var configuredLimit = new Size(_indexSearcher.MaxMemoizationSizeInBytes, SizeUnit.Bytes);
                var reason = _buffer.MaxAllocationInBytes < _indexSearcher.MaxMemoizationSizeInBytes
                    ? $"which exceeds the internal allocation maximum ('Indexing.Corax.MaxMemoizationSizeInMb' is configured to: {configuredLimit})"
                    : $"but 'Indexing.Corax.MaxMemoizationSizeInMb' is set to: {configuredLimit}";

                throw new InvalidOperationException(
                    $"Memoization clause needs more than {new Size(_buffer.MaxAllocationInBytes, SizeUnit.Bytes)} to fit its results, {reason}, in query: {inner}");
            }
        }

        public void Score(Span<long> matches, Span<float> scores, float boostFactor)
        {
            _inner.Score(matches, scores, boostFactor);
        }

        public QueryInspectionNode Inspect()
        {
            return _inner.Inspect();
        }
        string DebugView => Inspect().ToString();

        public void InnerRetriever(out TInner inner) => inner = _inner;

        public void Dispose()
        {
            if (_disposed)
                return;
            
            _disposed = true;
            _buffer.Dispose();
        }
    }
}
