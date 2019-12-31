using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Documents.TimeSeries
{
    public unsafe class TimeSeriesStorage
    {
        public const int MaxSegmentSize = 2048;

        public static readonly Slice AllTimeSeriesEtagSlice;

        private static Slice CollectionTimeSeriesEtagsSlice;

        private static readonly Slice TimeSeriesKeysSlice;

        private static readonly TableSchema TimeSeriesSchema = new TableSchema
        {
            TableType = (byte)TableType.TimeSeries
        };

        private readonly DocumentDatabase _documentDatabase;
        private readonly DocumentsStorage _documentsStorage;

        private HashSet<string> _tableCreated = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        static TimeSeriesStorage()
        {
            using (StorageEnvironment.GetStaticContext(out ByteStringContext ctx))
            {
                Slice.From(ctx, "AllTimeSeriesEtag", ByteStringType.Immutable, out AllTimeSeriesEtagSlice);
                Slice.From(ctx, "CollectionTimeSeriesEtags", ByteStringType.Immutable, out CollectionTimeSeriesEtagsSlice);
                Slice.From(ctx, "TimeSeriesKeys", ByteStringType.Immutable, out TimeSeriesKeysSlice);
            }

            TimeSeriesSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)TimeSeriesTable.TimeSeriesKey,
                Count = 1,
                Name = TimeSeriesKeysSlice,
                IsGlobal = true
            });

            TimeSeriesSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)TimeSeriesTable.Etag,
                Name = AllTimeSeriesEtagSlice,
                IsGlobal = true
            });

            TimeSeriesSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)TimeSeriesTable.Etag,
                Name = CollectionTimeSeriesEtagsSlice
            });
        }


        public TimeSeriesStorage(DocumentDatabase documentDatabase, Transaction tx)
        {
            _documentDatabase = documentDatabase;
            _documentsStorage = documentDatabase.DocumentsStorage;

            tx.CreateTree(TimeSeriesKeysSlice);
        }

        public string RemoveTimestampRange(DocumentsOperationContext context, string documentId, string collection, string name, DateTime from, DateTime to)
        {
            CollectionName collectionName = _documentsStorage.ExtractCollectionName(context, collection);
            Table table = GetTimeSeriesTable(context.Transaction.InnerTransaction, collectionName);

            from = EnsureMillisecondsPrecision(from);
            to = EnsureMillisecondsPrecision(to);
            
            using (DocumentIdWorker.GetSliceFromId(context, documentId, out Slice documentKeyPrefix, SpecialChars.RecordSeparator))
            using (DocumentIdWorker.GetLower(context.Allocator, name, out Slice timeSeriesName))
            using (context.Allocator.Allocate(documentKeyPrefix.Size + timeSeriesName.Size + 1 /* separator */ + sizeof(long) /*  segment start */,
                out ByteString timeSeriesKeyBuffer))
            using (CreateTimeSeriesKeyPrefixSlice(context, timeSeriesKeyBuffer, documentKeyPrefix, timeSeriesName, out Slice timeSeriesPrefixSlice))
            using (CreateTimeSeriesKeySlice(context, timeSeriesKeyBuffer, timeSeriesPrefixSlice, from, out Slice timeSeriesKeySlice))
            {
                // first try to find the previous segment containing from value
                if (table.SeekOneBackwardByPrimaryKeyPrefix(timeSeriesPrefixSlice, timeSeriesKeySlice, out var segmentValueReader) == false)
                {
                    // or the first segment _after_ the from value
                    if (table.SeekOnePrimaryKeyWithPrefix(timeSeriesPrefixSlice, timeSeriesKeySlice, out segmentValueReader) == false)
                        return null;
                }

                string changeVector = null;

                while (true)
                {
                    if (TryRemoveRange(ref segmentValueReader, out DateTime lastBaseline) == false)
                    {
                        RemoveTimeSeriesNameFromMetadata(context, documentId, name);
                        if (changeVector != null)
                            Notify();

                        return changeVector;
                    }

                    var upcomingSegment = TryGetNextSegment(lastBaseline);
                    if (upcomingSegment == null)
                    {
                        RemoveTimeSeriesNameFromMetadata(context, documentId, name);
                        Notify();
                        return changeVector;
                    }

                    segmentValueReader = upcomingSegment.Reader;
                }

                void Notify()
                {
                    context.Transaction.AddAfterCommitNotification(new TimeSeriesChange
                    {
                        ChangeVector = changeVector,
                        DocumentId = documentId,
                        Name = name,
                        Type = TimeSeriesChangeTypes.Delete,
                        From = from,
                        To = to,
                        CollectionName = collectionName.Name
                    });
                }

                bool TryRemoveRange(ref TableValueReader reader, out DateTime next)
                {
                    next = default;

                    var key = reader.Read((int)TimeSeriesTable.TimeSeriesKey, out int keySize);
                    var baselineMilliseconds = Bits.SwapBytes(
                        *(long*)(key + keySize - sizeof(long))
                    );
                    var ticks = baselineMilliseconds * 10_000;
                    var baseline = new DateTime(ticks);

                    if (baseline > to)
                        return false; // we got to the end

                    using (var holder = new TimeSeriesSegmentHolder(this, context, documentId, name, collectionName, baseline))
                    {
                        if (holder.LoadCurrentSegment() == false)
                            return false;

                        var readOnlySegment = holder.ReadOnlySegment;
                        var end = readOnlySegment.GetLastTimestamp(baseline);

                        if (baseline > end)
                            return false;

                        var newSegment = new TimeSeriesValuesSegment(holder.Allocator.Buffer.Ptr, MaxSegmentSize);
                        newSegment.Initialize(readOnlySegment.NumberOfValues);

                        using (var enumerator = readOnlySegment.GetEnumerator(context.Allocator))
                        {
                            var state = new TimeStampState[readOnlySegment.NumberOfValues];
                            var values = new double[readOnlySegment.NumberOfValues];
                            var tag = new TimeSeriesValuesSegment.TagPointer();

                            while (enumerator.MoveNext(out int ts, values, state, ref tag, out var status))
                            {
                                var current = baseline.AddMilliseconds(ts);

                                if (current >= from && current <= to)
                                {
                                    status = TimeSeriesValuesSegment.Dead;
                                }

                                holder.AddValue(current, values, tag.AsSpan(), ref newSegment, status);
                            }
                        }

                        holder.AppendExistingSegment(newSegment);
                        changeVector = holder.ChangeVector;
                        next = holder.BaselineDate;
                        return next < end;
                    }
                }

                Table.TableValueHolder TryGetNextSegment(DateTime baseline)
                {
                    var offset = timeSeriesKeySlice.Size - sizeof(long);
                    *(long*)(timeSeriesKeySlice.Content.Ptr + offset) = Bits.SwapBytes(baseline.Ticks / 10_000);

                    foreach (var (_, tvh) in table.SeekByPrimaryKeyPrefix(timeSeriesKeySlice, timeSeriesKeySlice, 0))
                    {
                        return tvh;
                    }

                    return null;
                }

                void RemoveTimeSeriesNameFromMetadata(DocumentsOperationContext ctx, string docId, string tsName)
                {
                    var doc = _documentDatabase.DocumentsStorage.Get(ctx, docId);
                    if (doc == null)
                        return;

                    var data = doc.Data;
                    var flags = doc.Flags.Strip(DocumentFlags.FromClusterTransaction | DocumentFlags.Resolved);

                    BlittableJsonReaderArray tsNames = null;
                    if (doc.TryGetMetadata(out var metadata))
                    {
                        metadata.TryGet(Constants.Documents.Metadata.TimeSeries, out tsNames);
                    }

                    if (metadata == null || tsNames == null)
                        return;

                    var tsNamesList = new List<string>(tsNames.Length + 1);
                    for (var i = 0; i < tsNames.Length; i++)
                    {
                        var val = tsNames.GetStringByIndex(i);
                        if (val == null)
                            continue;
                        tsNamesList.Add(val);
                    }

                    var location = tsNames.BinarySearch(tsName, StringComparison.Ordinal);
                    if (location < 0)
                        return;

                    tsNamesList.RemoveAt(location);

                    data.Modifications = new DynamicJsonValue(data);
                    metadata.Modifications = new DynamicJsonValue(metadata);

                    if (tsNamesList.Count == 0)
                    {
                        metadata.Modifications.Remove(Constants.Documents.Metadata.TimeSeries);
                        flags.Strip(DocumentFlags.HasTimeSeries);
                    }
                    else
                    {
                        metadata.Modifications[Constants.Documents.Metadata.TimeSeries] = tsNamesList;
                    }

                    data.Modifications[Constants.Documents.Metadata.Key] = metadata;

                    using (data)
                    {
                        var newDocumentData = ctx.ReadObject(doc.Data, docId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                        _documentDatabase.DocumentsStorage.Put(ctx, docId, null, newDocumentData, flags: flags);
                    }
                }
            }
        }

        private static DateTime EnsureMillisecondsPrecision(DateTime dt)
        {
            var remainder = dt.Ticks % 10_000;
            if (remainder != 0)
                dt = dt.AddTicks(-remainder);

            return dt;
        }

        public void DeleteTimeSeriesForDocument(DocumentsOperationContext context, string documentId, CollectionName collection)
        {
            // this will be called as part of document's delete

            var table = GetTimeSeriesTable(context.Transaction.InnerTransaction, collection);

            using (DocumentIdWorker.GetSliceFromId(context, documentId, out Slice documentKeyPrefix, SpecialChars.RecordSeparator))
            {
                table.DeleteByPrimaryKeyPrefix(documentKeyPrefix);
            }
        }


        public Reader GetReader(DocumentsOperationContext context, string documentId, string name, DateTime from, DateTime to)
        {
            return new Reader(context, documentId, name, from, to);
        }

        public class Reader
        {
            private readonly DocumentsOperationContext _context;
            private readonly string _documentId;
            private readonly string _name;
            private readonly DateTime _from, _to;
            private readonly Table _table;
            internal TableValueReader _tvr;
            private double[] _values = Array.Empty<double>();
            private TimeStampState[] _states = Array.Empty<TimeStampState>();
            private TimeSeriesValuesSegment.TagPointer _tagPointer;
            private LazyStringValue _tag;
            private TimeSeriesValuesSegment _currentSegment;

            public Reader(DocumentsOperationContext context, string documentId, string name, DateTime from, DateTime to)
            {
                _context = context;
                _documentId = documentId;
                _name = name;
                _table = new Table(TimeSeriesSchema, context.Transaction.InnerTransaction);
                _tag = new LazyStringValue(null, null, 0, context);

                _from = EnsureMillisecondsPrecision(from);
                _to = EnsureMillisecondsPrecision(to);
            }

            internal bool Init()
            {
                using (DocumentIdWorker.GetSliceFromId(_context, _documentId, out Slice documentKeyPrefix, SpecialChars.RecordSeparator))
                using (DocumentIdWorker.GetLower(_context.Allocator, _name, out Slice timeSeriesName))
                using (_context.Allocator.Allocate(documentKeyPrefix.Size + timeSeriesName.Size + 1 /* separator */ + sizeof(long) /*  segment start */,
                    out ByteString timeSeriesKeyBuffer))
                using (CreateTimeSeriesKeyPrefixSlice(_context, timeSeriesKeyBuffer, documentKeyPrefix, timeSeriesName, out Slice timeSeriesPrefixSlice))
                using (CreateTimeSeriesKeySlice(_context, timeSeriesKeyBuffer, timeSeriesPrefixSlice, _from, out Slice timeSeriesKeySlice))
                {
                    if (_table.SeekOneBackwardByPrimaryKeyPrefix(timeSeriesPrefixSlice, timeSeriesKeySlice, out _tvr) == false)
                    {
                        return _table.SeekOnePrimaryKeyWithPrefix(timeSeriesPrefixSlice, timeSeriesKeySlice, out _tvr);
                    }

                    return true;
                }
            }

            public class SingleResult
            {
                public DateTime TimeStamp;
                public Memory<double> Values;
                public LazyStringValue Tag;
                public ulong Status;
            }

            public class SegmentResult
            {
                public DateTime Start, End;
                public StatefulTimeStampValueSpan Summary;
                private Reader _reader;

                public SegmentResult(Reader reader)
                {
                    _reader = reader;
                }

                public IEnumerable<SingleResult> Values => _reader.YieldSegment(Start);

            }


            internal class SeriesSummary
            {
                public SeriesSummary(int numberOfValues)
                {
                    Min = new double[numberOfValues];
                    Max = new double[numberOfValues];
                }

                public int Count { get; set; }

                public double[] Min { get; set; }

                public double[] Max { get; set; }

            }

            internal SeriesSummary GetSummary()
            {
                if (Init() == false)
                    return null;

                InitializeSegment(out _, out _currentSegment);

                var result = new SeriesSummary(_currentSegment.NumberOfValues);

                do
                {
                    if (_currentSegment.NumberOfEntries == 0)
                        continue;

                    for (int i = 0; i < _currentSegment.NumberOfValues; i++)
                    {
                        if (result.Count == 0)
                        {
                            result.Min[i] = _currentSegment.SegmentValues.Span[i].Min;
                            result.Max[i] = _currentSegment.SegmentValues.Span[i].Max;
                            continue;
                        }

                        if (double.IsNaN(_currentSegment.SegmentValues.Span[i].Min) == false)
                        {
                            result.Min[i] = Math.Min(result.Min[i], _currentSegment.SegmentValues.Span[i].Min);
                        }

                        if (double.IsNaN(_currentSegment.SegmentValues.Span[i].Max) == false)
                        {
                            result.Max[i] = Math.Max(result.Max[i], _currentSegment.SegmentValues.Span[i].Max);
                        }
                    }

                    result.Count += _currentSegment.SegmentValues.Span[0].Count;

                } while (NextSegment(out _));

                return result;
            }

            public IEnumerable<(IEnumerable<SingleResult> IndividualValues, SegmentResult Segment)> SegmentsOrValues()
            {
                if (Init() == false)
                    yield break;

                var segmentResult = new SegmentResult(this);
                InitializeSegment(out var baselineMilliseconds, out _currentSegment);


                while (true)
                {
                    var baseline = new DateTime(baselineMilliseconds * 10_000, DateTimeKind.Utc);

                    if (_currentSegment.NumberOfValues > _values.Length)
                    {
                        _values = new double[_currentSegment.NumberOfValues];
                        _states = new TimeStampState[_currentSegment.NumberOfValues];
                    }

                    segmentResult.End = _currentSegment.GetLastTimestamp(baseline);
                    segmentResult.Start = baseline;

                    if (segmentResult.Start >= _from && 
                        segmentResult.End <= _to && 
                        _currentSegment.NumberOfEntries > 0)
                    {
                        // we can yield the whole segment in one go
                        segmentResult.Summary = _currentSegment.SegmentValues;
                        yield return (null, segmentResult);
                    }
                    else
                    {
                        yield return (YieldSegment(baseline), default);
                    }

                    if (NextSegment(out baselineMilliseconds) == false)
                        yield break;

                }
            }

            public IEnumerable<SingleResult> AllValues()
            {
                if (Init() == false)
                    yield break;

                InitializeSegment(out var baselineMilliseconds, out _currentSegment);
                while (true)
                {
                    var baseline = new DateTime(baselineMilliseconds * 10_000, DateTimeKind.Utc);

                    if (_currentSegment.NumberOfEntries > 0)
                    {
                        if (_currentSegment.NumberOfValues > _values.Length)
                        {
                            _values = new double[_currentSegment.NumberOfValues];
                            _states = new TimeStampState[_currentSegment.NumberOfValues];
                        }

                        foreach (var val in YieldSegment(baseline))
                        {
                            yield return val;
                        }
                    }

                    if (NextSegment(out baselineMilliseconds) == false)
                        yield break;
                }
            }

            private IEnumerable<SingleResult> YieldSegment(DateTime baseline)
            {
                var result = new SingleResult();

                if (_currentSegment.NumberOfEntries == 0)
                    yield break;

                using (var enumerator = _currentSegment.GetEnumerator(_context.Allocator))
                {
                    while (enumerator.MoveNext(out int ts, _values, _states, ref _tagPointer, out var status))
                    {
                        if (status == TimeSeriesValuesSegment.Dead)
                            continue;

                        var cur = baseline.AddMilliseconds(ts);

                        if (cur > _to)
                            yield break;

                        if (cur < _from)
                            continue;

                        var tag = SetTimestampTag();

                        var end = _values.Length;
                        while (end >= 0 && double.IsNaN(_values[end - 1]))
                        {
                            end--;
                        }
                        result.TimeStamp = cur;
                        result.Tag = tag;
                        result.Status = status;
                        result.Values = new Memory<double>(_values, 0, end);

                        yield return result;
                    }
                }
            }

            private LazyStringValue SetTimestampTag()
            {
                if (_tagPointer.Pointer == null)
                {
                    return null;
                }
                var lazyStringLen = BlittableJsonReaderBase.ReadVariableSizeInt(_tagPointer.Pointer, 0, out var offset);
                _tag.Renew(null, _tagPointer.Pointer + offset, lazyStringLen);
                return _tag;
            }

            private bool NextSegment(out long baselineMilliseconds)
            {
                byte* key = _tvr.Read((int)TimeSeriesTable.TimeSeriesKey, out int keySize);
                using (Slice.From(_context.Allocator, key, keySize - sizeof(long), out var prefix))
                using (Slice.From(_context.Allocator, key, keySize, out var current))
                {
                    foreach (var (nextKey, tvh) in _table.SeekByPrimaryKeyPrefix(prefix, current, 0))
                    {
                        _tvr = tvh.Reader;

                        InitializeSegment(out baselineMilliseconds, out _currentSegment);

                        return true;
                    }
                }

                baselineMilliseconds = default;
                _currentSegment = default;

                return false;
            }

            private void InitializeSegment(out long baselineMilliseconds, out TimeSeriesValuesSegment readOnlySegment)
            {
                byte* key = _tvr.Read((int)TimeSeriesTable.TimeSeriesKey, out int keySize);
                baselineMilliseconds = Bits.SwapBytes(
                    *(long*)(key + keySize - sizeof(long))
                );
                var segmentReadOnlyBuffer = _tvr.Read((int)TimeSeriesTable.Segment, out int size);
                readOnlySegment = new TimeSeriesValuesSegment(segmentReadOnlyBuffer, size);
            }
        }

        public bool TryAppendEntireSegment(DocumentsOperationContext context, TimeSeriesReplicationItem item, DateTime baseline)
        {
            var collectionName = _documentsStorage.ExtractCollectionName(context, item.Collection);
            return TryAppendEntireSegment(context, item.Key, collectionName, item.ChangeVector, item.Segment, baseline);
        }

        public bool TryAppendEntireSegment(DocumentsOperationContext context, Slice key, CollectionName collectionName, string changeVector, TimeSeriesValuesSegment segment, DateTime baseline)
        {
            var table = GetTimeSeriesTable(context.Transaction.InnerTransaction, collectionName);

            if (table.ReadByKey(key, out var tvr))
            {
                var existingChangeVector = DocumentsStorage.TableValueToChangeVector(context, (int)TimeSeriesTable.ChangeVector, ref tvr);
                var status = ChangeVectorUtils.GetConflictStatus(changeVector, existingChangeVector);

                if (status == ConflictStatus.AlreadyMerged)
                    return true; // nothing to do, we already have this

                if (status == ConflictStatus.Update)
                {
                    // we can put the segment directly only if the incoming segment doesn't overlap with any existing one 
                    using (Slice.From(context.Allocator, key.Content.Ptr, key.Size - sizeof(long), ByteStringType.Immutable, out var prefix))
                    {
                        if (IsOverlapWithHigherSegment(prefix) == false)
                        {
                            AppendEntireSegment();
                            return true;
                        }
                    }
                }

                return false;
            }

            // if this segment isn't overlap with any other we can put it directly
            using (Slice.From(context.Allocator, key.Content.Ptr, key.Size - sizeof(long), ByteStringType.Immutable, out var prefix))
            {
                if (IsOverlapWithHigherSegment(prefix) || IsOverlapWithLowerSegment(prefix))
                    return false;

                AppendEntireSegment();
                return true;
            }

            void AppendEntireSegment()
            {
                var newEtag = _documentsStorage.GenerateNextEtag();
                EnsureSegmentSize(segment.NumberOfBytes);

                using (Slice.From(context.Allocator, changeVector, out Slice cv))
                using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out var collectionSlice))
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(key);
                    tvb.Add(Bits.SwapBytes(newEtag));
                    tvb.Add(cv);
                    tvb.Add(segment.Ptr, segment.NumberOfBytes);
                    tvb.Add(collectionSlice);
                    tvb.Add(context.GetTransactionMarker());

                    table.Set(tvb);
                }
            }

            bool IsOverlapWithHigherSegment(Slice prefix)
            {
                var lastTimestamp = segment.GetLastTimestamp(baseline);
                var nextSegmentBaseline = BaselineOfNextSegment(table, prefix, key, baseline);
                return lastTimestamp >= nextSegmentBaseline;
            }

            bool IsOverlapWithLowerSegment(Slice prefix)
            {
                var myLastTimeStamp = segment.GetLastTimestamp(baseline);
                using (Slice.From(context.Allocator, key.Content.Ptr, key.Size, ByteStringType.Immutable, out var lastKey))
                {
                    *(long*)(lastKey.Content.Ptr + lastKey.Size - sizeof(long)) = Bits.SwapBytes(myLastTimeStamp.Ticks / 10_000);
                    if (table.SeekOneBackwardByPrimaryKeyPrefix(prefix, lastKey, out tvr) == false)
                    {
                        return false;
                    }
                }

                var segmentPtr = tvr.Read((int)TimeSeriesTable.Segment, out var size);
                var prevSegment = new TimeSeriesValuesSegment(segmentPtr, size);

                var keyPtr = tvr.Read((int)TimeSeriesTable.TimeSeriesKey, out size);
                var prevBaselineMs = Bits.SwapBytes(*(long*)(keyPtr + size - sizeof(long)));
                var prevBaseline = new DateTime(prevBaselineMs * 10_000);

                var last = prevSegment.GetLastTimestamp(prevBaseline);
                return last >= baseline;
            }
        }


        private static DateTime? BaselineOfNextSegment(TimeSeriesSegmentHolder segmentHolder, DateTime myDate)
        {
            var table = segmentHolder.Table;
            var prefix = segmentHolder.Allocator.TimeSeriesPrefixSlice;
            var key = segmentHolder.Allocator.TimeSeriesKeySlice;

            return BaselineOfNextSegment(table, prefix, key, myDate);
        }

        private static DateTime? BaselineOfNextSegment(Table table, Slice prefix, Slice key, DateTime myDate)
        {
            if (table.SeekOnePrimaryKeyWithPrefix(prefix, key, out var reader))
            {
                var currentKey = reader.Read((int)TimeSeriesTable.TimeSeriesKey, out var keySize);
                var baseline = Bits.SwapBytes(
                    *(long*)(currentKey + keySize - sizeof(long))
                );
                var date = new DateTime(baseline * 10_000);
                if (date > myDate)
                    return date;

                foreach (var (_, holder) in table.SeekByPrimaryKeyPrefix(prefix, key, 0))
                {
                    currentKey = holder.Reader.Read((int)TimeSeriesTable.TimeSeriesKey, out keySize);
                    baseline = Bits.SwapBytes(
                        *(long*)(currentKey + keySize - sizeof(long))
                    );
                    return new DateTime(baseline * 10_000);
                }
            }

            return null;
        }

        public class TimeSeriesSlicer : IDisposable
        {
            private readonly List<ByteStringContext.InternalScope> _internalScopesToDispose = new List<ByteStringContext.InternalScope>();
            private readonly List<ByteStringContext.ExternalScope> _externalScopesToDispose = new List<ByteStringContext.ExternalScope>();
            public ByteString Buffer, TimeSeriesKeyBuffer;
            public Slice TimeSeriesKeySlice, TimeSeriesPrefixSlice, TagSlice, TimeSeriesName;
            private string _docId;

            public TimeSeriesSlicer(DocumentsOperationContext context, string documentId, string name, DateTime timestamp, LazyStringValue tag = null)
            {
                _internalScopesToDispose.Add(DocumentIdWorker.GetSliceFromId(context, documentId, out Slice documentKeyPrefix, SpecialChars.RecordSeparator));
                _internalScopesToDispose.Add(DocumentIdWorker.GetLower(context.Allocator, name, out TimeSeriesName));
                _internalScopesToDispose.Add(context.Allocator.Allocate(documentKeyPrefix.Size + TimeSeriesName.Size + 1 /* separator */ + sizeof(long) /*  segment start */,
                    out TimeSeriesKeyBuffer));
                _externalScopesToDispose.Add(CreateTimeSeriesKeyPrefixSlice(context, TimeSeriesKeyBuffer, documentKeyPrefix, TimeSeriesName, out TimeSeriesPrefixSlice));
                _externalScopesToDispose.Add(CreateTimeSeriesKeySlice(context, TimeSeriesKeyBuffer, TimeSeriesPrefixSlice, timestamp, out TimeSeriesKeySlice));
                _internalScopesToDispose.Add(context.Allocator.Allocate(MaxSegmentSize, out Buffer));

                Memory.Set(Buffer.Ptr, 0, MaxSegmentSize);

                _docId = documentId;

                UpdateTagSlice(context, tag);
            }

            public void UpdateTagSlice(DocumentsOperationContext context, LazyStringValue tag)
            {
                if (tag == null)
                {
                    TagSlice = Slices.Empty;
                    return;
                }

                _internalScopesToDispose.Add(DocumentIdWorker.GetStringPreserveCase(context, tag, out TagSlice));

                if (TagSlice.Size > byte.MaxValue)
                    throw new ArgumentOutOfRangeException(nameof(tag),
                        $"Tag '{tag}' is too big (max 255 bytes) for document '{_docId}' in time series: {TimeSeriesName}");
            }

            public void Dispose()
            {
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
        }

        public class TimeSeriesSegmentHolder : IDisposable
        {
            private readonly TimeSeriesStorage _tss;
            private readonly DocumentsOperationContext _context;
            public readonly TimeSeriesSlicer Allocator;
            public readonly bool FromReplication;
            private readonly string _docId;
            private readonly CollectionName _collection;
            private readonly string _name;

            private TableValueReader _tvr;

            public long BaselineMilliseconds;
            public DateTime BaselineDate;
            public TimeSeriesValuesSegment ReadOnlySegment;

            private long _currentEtag;
            
            private string _currentChangeVector;
            public string ChangeVector => _currentChangeVector;

            private byte* _key;
            private int _keySize;
            private AllocatedMemoryData _clonedReadonlySegment;

            public TimeSeriesSegmentHolder(
                TimeSeriesStorage tss,
                DocumentsOperationContext context,
                string docId,
                string name,
                CollectionName collection,
                DateTime timeStamp
                )
            {
                _tss = tss;
                _context = context;
                _collection = collection;
                _docId = docId;
                _name = name;

                Allocator = new TimeSeriesSlicer(_context, docId, name, timeStamp);

                (_currentChangeVector, _currentEtag) = _tss.GenerateChangeVector(_context, null);
            }

            public TimeSeriesSegmentHolder(
                TimeSeriesStorage tss,
                DocumentsOperationContext context,
                TimeSeriesSlicer allocator,
                string docId,
                string name,
                CollectionName collection,
                string fromReplicationChangeVector)
            {
                _tss = tss;
                _context = context;
                Allocator = allocator;
                _collection = collection;
                _docId = docId;
                _name = name;
                FromReplication = fromReplicationChangeVector != null;

                (_currentChangeVector, _currentEtag) = _tss.GenerateChangeVector(_context, fromReplicationChangeVector);
            }

            private void Initialize()
            {
                Debug.Assert(_tvr.Equals(default) == false);

                _key = _tvr.Read((int)TimeSeriesTable.TimeSeriesKey, out _keySize);
                BaselineMilliseconds = Bits.SwapBytes(
                    *(long*)(_key + _keySize - sizeof(long))
                );

                BaselineDate = new DateTime(BaselineMilliseconds * 10_000);

                var segmentReadOnlyBuffer = _tvr.Read((int)TimeSeriesTable.Segment, out int size);

                Debug.Assert(_clonedReadonlySegment == null);
                // while appending or deleting, we might change the same segment. 
                // So we clone it.
                // for better reuse let use the const size of 'MaxSegmentSize'
                _clonedReadonlySegment = _context.GetMemory(MaxSegmentSize);
                Memory.Set(_clonedReadonlySegment.Address, 0, MaxSegmentSize);

                Memory.Copy(_clonedReadonlySegment.Address, segmentReadOnlyBuffer, size);
                ReadOnlySegment = new TimeSeriesValuesSegment(_clonedReadonlySegment.Address, size);
            }

            public Table Table => _tss.GetTimeSeriesTable(_context.Transaction.InnerTransaction, _collection);

            public void AppendExistingSegment(TimeSeriesValuesSegment newValueSegment)
            {
                EnsureSegmentSize(newValueSegment.NumberOfBytes);

                // the key came from the existing value, have to clone it
                using (Slice.From(_context.Allocator, _key, _keySize, out var keySlice))
                using (Table.Allocate(out var tvb))
                using (DocumentIdWorker.GetStringPreserveCase(_context, _collection.Name, out var collectionSlice))
                using (Slice.From(_context.Allocator, _currentChangeVector, out var cv))
                {
                    tvb.Add(keySlice);
                    tvb.Add(Bits.SwapBytes(_currentEtag));
                    tvb.Add(cv);
                    tvb.Add(newValueSegment.Ptr, newValueSegment.NumberOfBytes);
                    tvb.Add(collectionSlice);
                    tvb.Add(_context.GetTransactionMarker());

                    Table.Set(tvb);
                }

                (_currentChangeVector, _currentEtag) = _tss.GenerateChangeVector(_context, null);
            }

            public void AppendToNewSegment(ByteString buffer, Slice key, Span<double> valuesCopy, Slice tag, ulong status)
            {
                int deltaFromStart = 0;

                var newSegment = new TimeSeriesValuesSegment(buffer.Ptr, MaxSegmentSize);
                newSegment.Initialize(valuesCopy.Length);
                var tagSpan = tag.AsSpan();
                newSegment.Append(_context.Allocator, deltaFromStart, valuesCopy, tagSpan, status);

                EnsureSegmentSize(newSegment.NumberOfBytes);

                using (Slice.From(_context.Allocator, _currentChangeVector, out Slice cv))
                using (DocumentIdWorker.GetStringPreserveCase(_context, _collection.Name, out var collectionSlice))
                using (Table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(key);
                    tvb.Add(Bits.SwapBytes(_currentEtag));
                    tvb.Add(cv);
                    tvb.Add(newSegment.Ptr, newSegment.NumberOfBytes);
                    tvb.Add(collectionSlice);
                    tvb.Add(_context.GetTransactionMarker());

                    Table.Insert(tvb);
                }

                (_currentChangeVector, _currentEtag) = _tss.GenerateChangeVector(_context, null);
            }

            public void AddValue(Reader.SingleResult result, ref TimeSeriesValuesSegment segment, ulong status)
            {
                using (DocumentIdWorker.GetStringPreserveCase(_context, result.Tag, out var tagSlice))
                {
                    AddValue(result.TimeStamp,result.Values.Span, tagSlice.AsSpan(), ref segment, status);
                }
            }

            public void AddValue(DateTime time, Span<double> values, Span<byte> tagSlice, ref TimeSeriesValuesSegment segment, ulong status)
            {
                var timestampDiff = (int)((time - BaselineDate).Ticks / 10_000);
                if (segment.Append(_context.Allocator, timestampDiff, values, tagSlice, status) == false)
                {
                    FlushCurrentSegment(ref segment, values, tagSlice, status);
                    UpdateBaseline(timestampDiff);
                }
            }

            public bool LoadCurrentSegment()
            {
                if (Table.SeekOneBackwardByPrimaryKeyPrefix(Allocator.TimeSeriesPrefixSlice, Allocator.TimeSeriesKeySlice, out _tvr))
                {
                    Initialize();
                    return true;
                }

                return false;
            }

            private void FlushCurrentSegment(
                ref TimeSeriesValuesSegment splitSegment,
                Span<double> currentValues,
                Span<byte> currentTag,
                ulong status)
            {
                AppendExistingSegment(splitSegment);

                (_currentChangeVector, _currentEtag) = _tss.GenerateChangeVector(_context, null);

                splitSegment.Initialize(currentValues.Length);

                var result = splitSegment.Append(_context.Allocator, 0, currentValues, currentTag, status);
                if (result == false)
                    throw new InvalidOperationException($"After renewal of segment, was unable to append a new value. Shouldn't happen. Doc: {_docId}, name: {_name}");
            }

            public void UpdateBaseline(int timestampDiff)
            {
                Debug.Assert(timestampDiff > 0);
                BaselineDate = BaselineDate.AddMilliseconds(timestampDiff);
                BaselineMilliseconds = BaselineDate.Ticks / 10_000;

                *(long*)(Allocator.TimeSeriesKeyBuffer.Ptr + Allocator.TimeSeriesKeyBuffer.Length - sizeof(long)) = Bits.SwapBytes(BaselineMilliseconds);
                _key = Allocator.TimeSeriesKeyBuffer.Ptr;
                _keySize = Allocator.TimeSeriesKeyBuffer.Length;
            }

            public void Dispose()
            {
                Allocator.Dispose();

                if (_clonedReadonlySegment != null)
                {
                    _context.ReturnMemory(_clonedReadonlySegment);
                }
            }
        }

        public string AppendTimestamp(
            DocumentsOperationContext context,
            string documentId,
            string collection,
            string name,
            IEnumerable<TimeSeriesOperation.AppendOperation> toAppend,
            string changeVectorFromReplication = null)
        {
            var holder = new Reader.SingleResult();

            return AppendTimestamp(context, documentId, collection, name, toAppend.Select(ToResult), changeVectorFromReplication);

            Reader.SingleResult ToResult(TimeSeriesOperation.AppendOperation element)
            {
                holder.Values = element.Values;
                holder.Tag = context.GetLazyString(element.Tag);
                holder.TimeStamp = element.Timestamp;
                holder.Status = TimeSeriesValuesSegment.Live;
                return holder;
            }
        }

        public string AppendTimestamp(
            DocumentsOperationContext context,
            string documentId,
            string collection,
            string name,
            IEnumerable<Reader.SingleResult> toAppend,
            string changeVectorFromReplication = null
            )
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                Debug.Assert(false); // never hit
            }

            var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
            var newSeries = false;

            using (var appendEnumerator = toAppend.GetEnumerator())
            {
                while (appendEnumerator.MoveNext())
                {
                    var retry = true;
                    while (retry)
                    {
                        retry = false;
                        var current = appendEnumerator.Current;
                        Debug.Assert(current != null);

                        current.TimeStamp = EnsureMillisecondsPrecision(current.TimeStamp);

                        using (var slicer = new TimeSeriesSlicer(context, documentId, name, current.TimeStamp, current.Tag))
                        {
                            var segmentHolder = new TimeSeriesSegmentHolder(this, context, slicer, documentId, name, collectionName, changeVectorFromReplication);
                            if (segmentHolder.LoadCurrentSegment() == false)
                            {
                                // no matches for this series at all, need to create new segment
                                segmentHolder.AppendToNewSegment(slicer.Buffer, slicer.TimeSeriesKeySlice, current.Values.Span, slicer.TagSlice, current.Status);
                                newSeries = true;
                                break;
                            }

                            var segment = segmentHolder.ReadOnlySegment;

                            if (segment.NumberOfValues != current.Values.Length)
                            {
                                if (segment.NumberOfValues > current.Values.Length)
                                {
                                    using (context.Allocator.Allocate(segment.NumberOfValues * sizeof(double), out ByteString fullValuesBuffer))
                                    {
                                        var fullValues = new Span<double>(fullValuesBuffer.Ptr, segment.NumberOfValues);
                                        current.Values.Span.CopyTo(fullValues);
                                        for (int i = current.Values.Length; i < fullValues.Length; i++)
                                        {
                                            fullValues[i] = double.NaN;
                                        }

                                        current.Values = new Memory<double>(fullValues.ToArray());
                                    }
                                }
                                else
                                {
                                    // need to re-write the segment with the increased size
                                    // have to take into account that the segment will split because of this
                                    retry = SplitSegment(context, segmentHolder, appendEnumerator, current, updatedValuesNewSize: current.Values.Length - segment.NumberOfValues);
                                    continue;
                                }
                            }

                            if (TryAppendToCurrentSegment(context, segmentHolder, appendEnumerator, out var newValueFetched))
                                break;

                            if (newValueFetched)
                            {
                                retry = true;
                                continue;
                            }

                            retry = SplitSegment(context, segmentHolder, appendEnumerator, current, updatedValuesNewSize: 0);
                        }
                    }
                }
            }

            if (newSeries)
            {
                AddTimeSeriesNameToMetadata(context, documentId, name);
            }

            context.Transaction.AddAfterCommitNotification(new TimeSeriesChange
            {
                CollectionName = collectionName.Name,
                ChangeVector = context.LastDatabaseChangeVector,
                DocumentId = documentId,
                Name = name,
                Type = TimeSeriesChangeTypes.Put,
                From = DateTime.MinValue,
                To = DateTime.MaxValue
            });

            return context.LastDatabaseChangeVector;
        }

        private bool TryAppendToCurrentSegment(
            DocumentsOperationContext context,
            TimeSeriesSegmentHolder segmentHolder,
            IEnumerator<Reader.SingleResult> appendEnumerator,
            out bool newValueFetched)
        {
            var segment = segmentHolder.ReadOnlySegment;
            var slicer = segmentHolder.Allocator;

            var current = appendEnumerator.Current;
            var lastTimeStamp = segment.GetLastTimestamp(segmentHolder.BaselineDate);
            var nextSegmentBaseline = BaselineOfNextSegment(segmentHolder, current.TimeStamp) ?? DateTime.MaxValue;

            TimeSeriesValuesSegment newSegment = default;
            newValueFetched = false;
            while (true)
            {
                var canAppend = current.TimeStamp > lastTimeStamp;
                var deltaInMs = (current.TimeStamp.Ticks / 10_000) - segmentHolder.BaselineMilliseconds;

                if (canAppend &&
                    deltaInMs < int.MaxValue) // if the range is too big (over 24.85 days, using ms precision), we need a new segment
                {
                    // this is the simplest scenario, we can just add it.
                    if (newValueFetched == false)
                    {
                        segment.CopyTo(slicer.Buffer.Ptr);
                        newSegment = new TimeSeriesValuesSegment(slicer.Buffer.Ptr, MaxSegmentSize);
                    }

                    // checking if we run out of space here, in which can we'll create new segment
                    if (newSegment.Append(context.Allocator, (int)deltaInMs, current.Values.Span, slicer.TagSlice.AsSpan(), current.Status))
                    {
                        newValueFetched = true;
                        current = GetNext(appendEnumerator);

                        bool unchangedNumberOfValues = segment.NumberOfValues == current?.Values.Length;
                        if (current?.TimeStamp < nextSegmentBaseline && unchangedNumberOfValues)
                        {
                            slicer.UpdateTagSlice(context, current.Tag);
                            continue;
                        }

                        canAppend = false;
                    }
                }

                if (newValueFetched)
                {
                    segmentHolder.AppendExistingSegment(newSegment);
                }
                else if (canAppend)
                {
                    // either the range is too high to fit in a single segment (~50 days) or the 
                    // previous segment is full, we can just create a completely new segment with the 
                    // new value
                    segmentHolder.AppendToNewSegment(slicer.Buffer, slicer.TimeSeriesKeySlice, current.Values.Span, slicer.TagSlice, current.Status);
                    return true;
                }

                return current == null;
            }
        }

        private bool SplitSegment(
            DocumentsOperationContext context,
            TimeSeriesSegmentHolder timeSeriesSegment,
            IEnumerator<Reader.SingleResult> reader,
            Reader.SingleResult current,
            int updatedValuesNewSize)
        {
            // here we have a complex scenario, we need to add it in the middle of the current segment
            // to do that, we have to re-create it from scratch.

            // the first thing to do here it to copy the segment out, because we may be writing it in multiple
            // steps, and move the actual values as we do so

            var nextSegmentBaseline = BaselineOfNextSegment(timeSeriesSegment, current.TimeStamp);
            var segmentToSplit = timeSeriesSegment.ReadOnlySegment;
            var changed = false;

            using (context.Allocator.Allocate(segmentToSplit.NumberOfBytes, out var currentSegmentBuffer))
            {
                Memory.Copy(currentSegmentBuffer.Ptr, segmentToSplit.Ptr, segmentToSplit.NumberOfBytes);
                var readOnlySegment = new TimeSeriesValuesSegment(currentSegmentBuffer.Ptr, segmentToSplit.NumberOfBytes);

                var splitSegment = new TimeSeriesValuesSegment(timeSeriesSegment.Allocator.Buffer.Ptr, MaxSegmentSize);
                splitSegment.Initialize(current.Values.Span.Length);

                using (context.Allocator.Allocate((readOnlySegment.NumberOfValues + updatedValuesNewSize) * sizeof(double), out var valuesBuffer))
                using (context.Allocator.Allocate(readOnlySegment.NumberOfValues * sizeof(TimeStampState), out var stateBuffer))
                {
                    Memory.Set(valuesBuffer.Ptr, 0, valuesBuffer.Length);
                    Memory.Set(stateBuffer.Ptr, 0, stateBuffer.Length);

                    var currentValues = new Span<double>(valuesBuffer.Ptr, readOnlySegment.NumberOfValues);
                    var updatedValues = new Span<double>(valuesBuffer.Ptr, readOnlySegment.NumberOfValues + updatedValuesNewSize);
                    var state = new Span<TimeStampState>(stateBuffer.Ptr, readOnlySegment.NumberOfValues);
                    var currentTag = new TimeSeriesValuesSegment.TagPointer();

                    for (int i = readOnlySegment.NumberOfValues; i < readOnlySegment.NumberOfValues + updatedValuesNewSize; i++)
                    {
                        updatedValues[i] = double.NaN;
                    }

                    using (var enumerator = readOnlySegment.GetEnumerator(context.Allocator))
                    {
                        var originalBaseline = timeSeriesSegment.BaselineDate;
                        while (enumerator.MoveNext(out var currentTimestamp, currentValues, state, ref currentTag, out var localStatus))
                        {
                            var currentTime = originalBaseline.AddMilliseconds(currentTimestamp);
                            while (true)
                            {
                                if (ShouldAddLocal(currentTime, currentValues, current, nextSegmentBaseline, timeSeriesSegment))
                                {
                                    timeSeriesSegment.AddValue(currentTime, updatedValues, currentTag.AsSpan(), ref splitSegment, localStatus);
                                    if (currentTime == current?.TimeStamp)
                                    {
                                        current = GetNext(reader);
                                    }
                                    break;
                                }

                                changed = true;
                                Debug.Assert(current != null);
                                timeSeriesSegment.AddValue(current, ref splitSegment, current.Status);

                                if (currentTime == current.TimeStamp)
                                {
                                    current = GetNext(reader);
                                    break; // the local value was overwritten
                                }
                                current = GetNext(reader);
                            }
                        }
                    }

                    var retryAppend = current != null;
                    if (retryAppend && (current.TimeStamp >= nextSegmentBaseline == false))
                    {
                        changed = true;
                        retryAppend = false;
                        timeSeriesSegment.AddValue(current, ref splitSegment, current.Status);
                    }

                    if (changed == false)
                        return retryAppend;

                    timeSeriesSegment.AppendExistingSegment(splitSegment);
                    return retryAppend;
                }
            }
        }
        private static bool ShouldAddLocal(DateTime localTime, Span<double> localValues, Reader.SingleResult remote, DateTime? nextSegmentBaseline, TimeSeriesSegmentHolder holder)
        {
            if (remote == null)
                return true;

            if (localTime < remote.TimeStamp)
                return true;

            if (remote.TimeStamp >= nextSegmentBaseline)
                return true;

            if (localTime == remote.TimeStamp)
            {
                return holder.FromReplication == false || // if not from replication, this value overrides
                       localValues.SequenceCompareTo(remote.Values.Span) > 0; // if from replication, the largest value wins
            }

            return false;
        }

        private Reader.SingleResult GetNext(IEnumerator<Reader.SingleResult> reader)
        {
            Reader.SingleResult next = null;
            if (reader.MoveNext())
            {
                next = reader.Current;
            }

            return next;
        }

        private void AddTimeSeriesNameToMetadata(DocumentsOperationContext ctx, string docId, string tsName)
        {
            var doc = _documentDatabase.DocumentsStorage.Get(ctx, docId);
            if (doc == null)
                return;

            var data = doc.Data;
            BlittableJsonReaderArray tsNames = null;
            if (doc.TryGetMetadata(out var metadata))
            {
                metadata.TryGet(Constants.Documents.Metadata.TimeSeries, out tsNames);
            }

            if (tsNames == null)
            {
                data.Modifications = new DynamicJsonValue(data);
                if (metadata == null)
                {
                    data.Modifications[Constants.Documents.Metadata.Key] =
                        new DynamicJsonValue { [Constants.Documents.Metadata.TimeSeries] = new[] { tsName } };
                }
                else
                {
                    metadata.Modifications = new DynamicJsonValue(metadata) { [Constants.Documents.Metadata.TimeSeries] = new[] { tsName } };
                    data.Modifications[Constants.Documents.Metadata.Key] = metadata;
                }
            }
            else
            {
                var tsNamesList = new List<string>(tsNames.Length + 1);
                for (var i = 0; i < tsNames.Length; i++)
                {
                    var val = tsNames.GetStringByIndex(i);
                    if (val == null)
                        continue;
                    tsNamesList.Add(val);
                }

                var location = tsNames.BinarySearch(tsName, StringComparison.OrdinalIgnoreCase);
                if (location >= 0)
                    return;

                tsNamesList.Insert(~location, tsName);

                data.Modifications = new DynamicJsonValue(data);

                metadata.Modifications = new DynamicJsonValue(metadata) { [Constants.Documents.Metadata.TimeSeries] = tsNamesList };

                data.Modifications[Constants.Documents.Metadata.Key] = metadata;
            }

            var flags = doc.Flags.Strip(DocumentFlags.FromClusterTransaction | DocumentFlags.Resolved);
            flags |= DocumentFlags.HasTimeSeries;

            using (data)
            {
                var newDocumentData = ctx.ReadObject(doc.Data, docId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                _documentDatabase.DocumentsStorage.Put(ctx, docId, null, newDocumentData, flags: flags);
            }
        }

        public IEnumerable<ReplicationBatchItem> GetSegmentsFrom(DocumentsOperationContext context, long etag)
        {
            var table = new Table(TimeSeriesSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(TimeSeriesSchema.FixedSizeIndexes[AllTimeSeriesEtagSlice], etag, 0))
            {
                yield return CreateTimeSeriesSegmentItem(context, ref result.Reader);
            }
        }

        private static ReplicationBatchItem CreateTimeSeriesSegmentItem(DocumentsOperationContext context, ref TableValueReader reader)
        {
            var etag = *(long*)reader.Read((int)TimeSeriesTable.Etag, out _);
            var changeVectorPtr = reader.Read((int)TimeSeriesTable.ChangeVector, out int changeVectorSize);
            var segmentPtr = reader.Read((int)TimeSeriesTable.Segment, out int segmentSize);

            var item = new TimeSeriesReplicationItem
            {
                Type = ReplicationBatchItem.ReplicationItemType.TimeSeriesSegment,
                ChangeVector = Encoding.UTF8.GetString(changeVectorPtr, changeVectorSize),
                Segment = new TimeSeriesValuesSegment(segmentPtr, segmentSize),
                Collection = DocumentsStorage.TableValueToId(context, (int)TimeSeriesTable.Collection, ref reader),
                Etag = Bits.SwapBytes(etag),
                TransactionMarker = DocumentsStorage.TableValueToShort((int)TimeSeriesTable.TransactionMarker, nameof(TimeSeriesTable.TransactionMarker), ref reader)
            };

            var keyPtr = reader.Read((int)TimeSeriesTable.TimeSeriesKey, out int keySize);
            item.ToDispose(Slice.From(context.Allocator, keyPtr, keySize, ByteStringType.Immutable, out item.Key));
            return item;
        }

        public TimeSeriesSegmentEntry GetTimeSeries(DocumentsOperationContext context, Slice key)
        {
            var table = new Table(TimeSeriesSchema, context.Transaction.InnerTransaction);

            if (table.ReadByKey(key, out var reader) == false)
                return null;

            return CreateTimeSeriesItem(context, ref reader);
        }

        public TimeSeriesSegmentEntry GetTimeSeries(DocumentsOperationContext context, long etag)
        {
            var table = new Table(TimeSeriesSchema, context.Transaction.InnerTransaction);
            var index = TimeSeriesSchema.FixedSizeIndexes[AllTimeSeriesEtagSlice];

            if (table.Read(context.Allocator, index, etag, out var tvr) == false)
                return null;

            return CreateTimeSeriesItem(context, ref tvr);
        }

        public IEnumerable<TimeSeriesSegmentEntry> GetTimeSeriesFrom(DocumentsOperationContext context, long etag)
        {
            var table = new Table(TimeSeriesSchema, context.Transaction.InnerTransaction);

            foreach (var result in table.SeekForwardFrom(TimeSeriesSchema.FixedSizeIndexes[AllTimeSeriesEtagSlice], etag, 0))
            {
                yield return CreateTimeSeriesItem(context, ref result.Reader);
            }
        }

        public IEnumerable<TimeSeriesSegmentEntry> GetTimeSeriesFrom(DocumentsOperationContext context, string collection, long etag, long take)
        {
            var collectionName = _documentsStorage.GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                yield break;

            var table = GetTimeSeriesTable(context.Transaction.InnerTransaction, collectionName);

            if (table == null)
                yield break;

            foreach (var result in table.SeekForwardFrom(TimeSeriesSchema.FixedSizeIndexes[CollectionTimeSeriesEtagsSlice], etag, skip: 0))
            {
                if (take-- <= 0)
                    yield break;

                yield return CreateTimeSeriesItem(context, ref result.Reader);
            }
        }

        private static TimeSeriesSegmentEntry CreateTimeSeriesItem(DocumentsOperationContext context, ref TableValueReader reader)
        {
            var etag = *(long*)reader.Read((int)TimeSeriesTable.Etag, out _);
            var changeVectorPtr = reader.Read((int)TimeSeriesTable.ChangeVector, out int changeVectorSize);
            var segmentPtr = reader.Read((int)TimeSeriesTable.Segment, out int segmentSize);
            var keyPtr = reader.Read((int)TimeSeriesTable.TimeSeriesKey, out int keySize);

            var key = new LazyStringValue(null, keyPtr, keySize, context);

            TimeSeriesValuesSegment.ParseTimeSeriesKey(keyPtr, keySize,  context, out var docId, out var name, out var baseline);

            return new TimeSeriesSegmentEntry
            {
                Key = key,
                DocId = docId,
                Name = name,
                ChangeVector = Encoding.UTF8.GetString(changeVectorPtr, changeVectorSize),
                Segment = new TimeSeriesValuesSegment(segmentPtr, segmentSize),
                SegmentSize = segmentSize,
                Collection = DocumentsStorage.TableValueToId(context, (int)TimeSeriesTable.Collection, ref reader),
                Baseline = baseline,
                Etag = Bits.SwapBytes(etag),
            };
        }

        internal Reader.SeriesSummary GetSeriesSummary(DocumentsOperationContext context, string documentId, string name)
        {
            var reader = GetReader(context, documentId, name, DateTime.MinValue, DateTime.MaxValue);
            return reader.GetSummary();
        }

        private (string ChangeVector, long NewEtag) GenerateChangeVector(DocumentsOperationContext context, string fromReplicationChangeVector)
        {
            var newEtag = _documentsStorage.GenerateNextEtag();
            string databaseChangeVector = context.LastDatabaseChangeVector ?? DocumentsStorage.GetDatabaseChangeVector(context);
            string changeVector = ChangeVectorUtils.TryUpdateChangeVector(_documentDatabase, databaseChangeVector, newEtag).ChangeVector;

            if (fromReplicationChangeVector != null)
            {
                changeVector = ChangeVectorUtils.MergeVectors(fromReplicationChangeVector, changeVector);
            }

            context.LastDatabaseChangeVector = changeVector;
            return (changeVector, newEtag);
        }

        private static void EnsureSegmentSize(int size)
        {
            if (size > MaxSegmentSize)
                throw new ArgumentOutOfRangeException("Attempted to write a time series segment that is larger (" + size + ") than the maximum size allowed.");
        }

        public long GetNumberOfTimeSeriesSegments(DocumentsOperationContext context)
        {
            var fstIndex = TimeSeriesSchema.FixedSizeIndexes[AllTimeSeriesEtagSlice];
            var fst = context.Transaction.InnerTransaction.FixedTreeFor(fstIndex.Name, sizeof(long));
            return fst.NumberOfEntries;
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

        private Table GetTimeSeriesTable(Transaction tx, CollectionName collection)
        {
            string tableName = collection.GetTableName(CollectionTableType.TimeSeries);

            if (tx.IsWriteTransaction && _tableCreated.Contains(collection.Name) == false)
            {
                // RavenDB-11705: It is possible that this will revert if the transaction
                // aborts, so we must record this only after the transaction has been committed
                // note that calling the Create() method multiple times is a noop
                TimeSeriesSchema.Create(tx, tableName, 16);
                tx.LowLevelTransaction.OnDispose += _ =>
                {
                    if (tx.LowLevelTransaction.Committed == false)
                        return;

                    // not sure if we can _rely_ on the tx write lock here, so let's be safe and create
                    // a new instance, just in case 
                    _tableCreated = new HashSet<string>(_tableCreated, StringComparer.OrdinalIgnoreCase)
                    {
                        collection.Name
                    };
                };
            }

            return tx.OpenTable(TimeSeriesSchema, tableName);
        }

        public DynamicJsonArray GetTimeSeriesLowerNamesForDocument(DocumentsOperationContext context, string docId)
        {
            var table = new Table(TimeSeriesSchema, context.Transaction.InnerTransaction);
            var list = new DynamicJsonArray();

            // here we need to find all of the time series names for a given document.

            // for example we have:
            // doc/heartbeats/123
            // doc/heartbeats/234
            // doc/heartbeats/666
            // doc/pulse/123
            // doc/pulse/54656

            // so we seek backwards, starting from the end.
            // extracting the last name and use it as a prefix for the next iteration

            var dummyHolder = new ByteStringStorage
            {
                Flags = ByteStringType.Mutable,
                Length = 0,
                Ptr = (byte*)0,
                Size = 0
            };
            var slice = new Slice(new ByteString(&dummyHolder));

            using (DocumentIdWorker.GetSliceFromId(context, docId, out var documentKeyPrefix, SpecialChars.RecordSeparator))
            using (DocumentIdWorker.GetSliceFromId(context, docId, out var last, SpecialChars.RecordSeparator + 1))
            {
                if (table.SeekOneBackwardByPrimaryKeyPrefix(documentKeyPrefix, last, out var reader) == false)
                    return list;

                var size = documentKeyPrefix.Size;
                bool excludeValueFromSeek;
                do
                {
                    var lowerName = GetLowerCasedNameAndUpdateSlice(ref reader, size, ref slice);
                    if (lowerName == null)
                    {
                        excludeValueFromSeek = true;
                        continue;
                    }

                    excludeValueFromSeek = false;
                    list.Add(lowerName);
                } while (table.SeekOneBackwardByPrimaryKeyPrefix(documentKeyPrefix, slice, out reader, excludeValueFromSeek));

                list.Items.Reverse();
                return list;
            }
        }

        private static string GetLowerCasedNameAndUpdateSlice(ref TableValueReader reader, int prefixSize, ref Slice slice)
        {
            var segmentPtr = reader.Read((int)TimeSeriesTable.Segment, out var size);
            var segment = new TimeSeriesValuesSegment(segmentPtr, size);
            var keyPtr = reader.Read((int)TimeSeriesTable.TimeSeriesKey, out size);

            for (int i = 0; i < segment.NumberOfValues; i++)
            {
                if (segment.SegmentValues.Span[i].Count > 0)
                {
                    var name = Encoding.UTF8.GetString(keyPtr + prefixSize, size - prefixSize - sizeof(long) - 1);
                    slice.Content._pointer->Ptr = keyPtr;
                    slice.Content._pointer->Length = size - sizeof(long);
                    slice.Content._pointer->Size = size - sizeof(long);
                    return name;
                }
            }

            // this segment is empty or marked as deleted, look for the next segment in this time-series
            slice.Content._pointer->Ptr = keyPtr;
            slice.Content._pointer->Length = size;
            slice.Content._pointer->Size = size;
            return null;
        }

        public long GetLastTimeSeriesEtag(DocumentsOperationContext context, string collection)
        {
            var collectionName = _documentsStorage.GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                return 0;

            var table = GetTimeSeriesTable(context.Transaction.InnerTransaction, collectionName);

            // ReSharper disable once UseNullPropagation
            if (table == null)
                return 0;

            var result = table.ReadLast(TimeSeriesSchema.FixedSizeIndexes[CollectionTimeSeriesEtagsSlice]);
            if (result == null)
                return 0;

            return DocumentsStorage.TableValueToEtag((int)TimeSeriesTable.Etag, ref result.Reader);
        }

        private enum TimeSeriesTable
        {
            // Format of this is:
            // lower document id, record separator, lower time series name, record separator, segment start  
            TimeSeriesKey = 0,
            Etag = 1,
            ChangeVector = 2,
            Segment = 3,
            Collection = 4,
            TransactionMarker = 5
        }
    }
}
