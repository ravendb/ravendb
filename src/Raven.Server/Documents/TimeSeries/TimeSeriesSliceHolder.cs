using System;
using System.Collections.Generic;
using System.Text;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;

namespace Raven.Server.Documents.TimeSeries
{
    public unsafe class TimeSeriesSliceHolder : IDisposable
    {
        private readonly DocumentsOperationContext _context;
        private readonly string _docId;
        private readonly string _name;

        private readonly Dictionary<LazyStringValue, Slice> CachedTags = new Dictionary<LazyStringValue, Slice>();
        private readonly List<ByteStringContext.InternalScope> _internalScopesToDispose = new List<ByteStringContext.InternalScope>();
        private readonly List<ByteStringContext.ExternalScope> _externalScopesToDispose = new List<ByteStringContext.ExternalScope>();

        public ByteString SegmentBuffer, TimeSeriesKeyBuffer;
        public Slice TimeSeriesKeySlice, TimeSeriesPrefixSlice, LowerTimeSeriesName, DocumentKeyPrefix, StatsKey, CollectionSlice;
        public DateTime CurrentBaseline;

        public TimeSeriesSliceHolder(DocumentsOperationContext context, string documentId, string name)
        {
            _context = context;
            _docId = documentId;
            _name = name;

            Initialize();
        }

        public TimeSeriesSliceHolder WithBaseline(DateTime time)
        {
            SetBaselineToKey(time);
            return this;
        }

        public void CreateSegmentBuffer()
        {
            _internalScopesToDispose.Add(_context.Allocator.Allocate(TimeSeriesStorage.MaxSegmentSize, out SegmentBuffer));
            Memory.Set(SegmentBuffer.Ptr, 0, TimeSeriesStorage.MaxSegmentSize);
        }

        public TimeSeriesSliceHolder WithEtag(long etag)
        {
            if (TimeSeriesKeySlice.Content.HasValue == false)
                _externalScopesToDispose.Add(Slice.External(_context.Allocator, TimeSeriesKeyBuffer.Ptr, TimeSeriesKeyBuffer.Length, out TimeSeriesKeySlice));

            *(long*)(TimeSeriesKeyBuffer.Ptr + TimeSeriesKeySlice.Size) = Bits.SwapBytes(etag);
            return this;
        }

        public TimeSeriesSliceHolder WithCollection(string collection)
        {
            _internalScopesToDispose.Add(DocumentIdWorker.GetStringPreserveCase(_context, collection, out CollectionSlice));
            return this;
        }

        public void Initialize()
        {
            _internalScopesToDispose.Add(DocumentIdWorker.GetSliceFromId(_context, _docId, out DocumentKeyPrefix, SpecialChars.RecordSeparator)); // documentId/
            _internalScopesToDispose.Add(DocumentIdWorker.GetLower(_context.Allocator, _name, out LowerTimeSeriesName));

            var keyBufferSize = DocumentKeyPrefix.Size + LowerTimeSeriesName.Size + 1 /* separator */ + sizeof(long) /*  segment start */;
            _internalScopesToDispose.Add(_context.Allocator.Allocate(keyBufferSize, out TimeSeriesKeyBuffer));

            _externalScopesToDispose.Add(CreateTimeSeriesKeyPrefixSlice(_context, TimeSeriesKeyBuffer, DocumentKeyPrefix, LowerTimeSeriesName,
                out TimeSeriesPrefixSlice)); // documentId/timeseries/

            _externalScopesToDispose.Add(Slice.External(_context.Allocator, TimeSeriesKeyBuffer, 0, DocumentKeyPrefix.Size + LowerTimeSeriesName.Size,
                out StatsKey)); // documentId/timeseries
        }

        public void SetBaselineToKey(DateTime time)
        {
            if (TimeSeriesKeySlice.Content.HasValue == false) // uninitialized
                _externalScopesToDispose.Add(CreateTimeSeriesKeySlice(_context, TimeSeriesKeyBuffer, TimeSeriesPrefixSlice, time,
                    out TimeSeriesKeySlice)); // documentId/timeseries/ticks

            var ms = time.Ticks / 10_000;
            *(long*)(TimeSeriesKeyBuffer.Ptr + TimeSeriesPrefixSlice.Size) = Bits.SwapBytes(ms);

            CurrentBaseline = time;
        }

        public Slice External(Slice slice, int offset, int length)
        {
            _externalScopesToDispose.Add(Slice.External(_context.Allocator, slice.Content, offset, length, out var external));
            return external;
        }

        public Span<byte> TagAsSpan(LazyStringValue tag)
        {
            if (tag == null)
            {
                return Slices.Empty.AsSpan();
            }

            if (CachedTags.TryGetValue(tag, out var tagSlice))
            {
                return tagSlice.AsSpan();
            }

            _internalScopesToDispose.Add(DocumentIdWorker.GetStringPreserveCase(_context, tag, out tagSlice));
            CachedTags[tag] = tagSlice;

            if (tagSlice.Size > byte.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(tag),
                    $"Tag '{tag}' is too big (max 255 bytes) for document '{_docId}' in time series: {_name}");

            return tagSlice.AsSpan();
        }

        public void Dispose()
        {
            CachedTags.Clear();

            foreach (var internalScope in _internalScopesToDispose)
            {
                internalScope.Dispose();
            }

            _internalScopesToDispose.Clear();

            foreach (var externalScope in _externalScopesToDispose)
            {
                externalScope.Dispose();
            }

            _externalScopesToDispose.Clear();
        }

        private static ByteStringContext<ByteStringMemoryCache>.ExternalScope CreateTimeSeriesKeySlice(DocumentsOperationContext context, ByteString buffer,
            Slice timeSeriesPrefixSlice, DateTime timestamp, out Slice timeSeriesKeySlice)
        {
            var scope = Slice.External(context.Allocator, buffer.Ptr, buffer.Length, out timeSeriesKeySlice);
            var ms = timestamp.Ticks / 10_000;
            *(long*)(buffer.Ptr + timeSeriesPrefixSlice.Size) = Bits.SwapBytes(ms);
            return scope;
        }

        private static ByteStringContext<ByteStringMemoryCache>.ExternalScope CreateTimeSeriesKeyPrefixSlice(DocumentsOperationContext context, ByteString buffer,
            Slice documentIdPrefix, Slice timeSeriesName, out Slice timeSeriesPrefixSlice)
        {
            documentIdPrefix.CopyTo(buffer.Ptr);
            timeSeriesName.CopyTo(buffer.Ptr + documentIdPrefix.Size);
            int pos = documentIdPrefix.Size + timeSeriesName.Size;
            buffer.Ptr[pos++] = SpecialChars.RecordSeparator;
            var scope = Slice.External(context.Allocator, buffer.Ptr, pos, out timeSeriesPrefixSlice);
            return scope;
        }
    }
}
