using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.TimeSeries
{
    public enum SingleResultType
    {
        Raw,
        RolledUp
    }

    public class SingleResult
    {
        public DateTime Timestamp;
        public Memory<double> Values;
        public LazyStringValue Tag;
        public ulong Status;
        public SingleResultType Type;

        public DynamicJsonValue ToTimeSeriesEntryJson(double scale = 1)
        {
            var values = new DynamicJsonArray();
            foreach (var t in Values.Span)
            {
                values.Add(scale * t);
            }

            return new DynamicJsonValue
            {
                [nameof(TimeSeriesEntry.Tag)] = Tag?.ToString(),
                [nameof(TimeSeriesEntry.Timestamp)] = Timestamp,
                [nameof(TimeSeriesEntry.Values)] = values,
                [nameof(TimeSeriesEntry.IsRollup)] = Type == SingleResultType.RolledUp,
            };
        }

        public void CopyTo(SingleResult dest)
        {
            dest.Status = Status;

            // destination is shorter
            if (dest.Values.Length < Values.Length)
                dest.Values = new double[Values.Length]; 

            // destination is larger, so we need to forget values from the previous entry
            for (int i = Values.Length; i < dest.Values.Length; i++)
            {
                dest.Values.Span[i] = 0;
            }

            Values.CopyTo(dest.Values);
            dest.Tag = Tag?.CloneOnSameContext();
            dest.Type = Type;
            dest.Timestamp = Timestamp;
        }
    }

    public class SegmentResult
    {
        public DateTime Start, End;
        public TimeSeriesValuesSegment Summary;
        public string ChangeVector;
        private readonly TimeSeriesReader _reader;

        public SegmentResult(TimeSeriesReader reader)
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

    public interface ITimeSeriesReader
    {
        public IEnumerable<(IEnumerable<SingleResult> IndividualValues, SegmentResult Segment)> SegmentsOrValues();

        public IEnumerable<SingleResult> AllValues(bool includeDead = false);

        public bool IsRaw { get; }
    }

    public unsafe class TimeSeriesReader : ITimeSeriesReader
    {
        private readonly DocumentsOperationContext _context;
        private readonly string _documentId;
        private readonly string _name;
        internal readonly DateTime _from, _to;
        private readonly Table _table;
        internal TableValueReader _tvr;
        private double[] _values = Array.Empty<double>();
        private TimestampState[] _states = Array.Empty<TimestampState>();
        private LazyStringValue _tag;
        private TimeSeriesValuesSegment _currentSegment;
        private TimeSpan? _offset;
        private DetailedSingleResult _details;
        private readonly CancellationToken _token;

        public bool IsRaw { get; }

        public TimeSeriesReader(DocumentsOperationContext context, string documentId, string name, DateTime from, DateTime to, TimeSpan? offset, CancellationToken token = default)
        {
            _context = context;
            _documentId = documentId;
            _name = name.ToLowerInvariant();
            _table = new Table(TimeSeriesStorage.TimeSeriesSchema, context.Transaction.InnerTransaction);
            _tag = new LazyStringValue(null, null, 0, context);
            _offset = offset;
            _token = token;

            _from = from;
            _to = to;
            IsRaw = _name.Contains(TimeSeriesConfiguration.TimeSeriesRollupSeparator) == false;
        }

        public void IncludeDetails()
        {
            _details = new DetailedSingleResult(_context.LastDatabaseChangeVector ?? DocumentsStorage.GetDatabaseChangeVector(_context));;
        }

        public DetailedSingleResult GetDetails => _details ?? throw new ArgumentNullException($"Forget to explicitly to 'IncludeDetails'?");

        internal bool Init()
        {
            if (_from > _to)
                return false;

            using (var holder = new TimeSeriesSliceHolder(_context, _documentId, _name).WithBaseline(_from))
            {
                if (_table.SeekOneBackwardByPrimaryKeyPrefix(holder.TimeSeriesPrefixSlice, holder.TimeSeriesKeySlice, out _tvr) == false)
                {
                    return _table.SeekOnePrimaryKeyWithPrefix(holder.TimeSeriesPrefixSlice, holder.TimeSeriesKeySlice, out _tvr);
                }

                return true;
            }
        }

        public SingleResult First()
        {
            return AllValues().FirstOrDefault();
        }

        public SingleResult Last(DateTime? to = null)
        {
            if (to.HasValue == false)
                to = _to;

            var date = to.Value;

            while (true)
            {
                using (var holder = new TimeSeriesSliceHolder(_context, _documentId, _name).WithBaseline(date))
                {
                    if (_table.SeekOneBackwardByPrimaryKeyPrefix(holder.TimeSeriesPrefixSlice, holder.TimeSeriesKeySlice, out _tvr) == false)
                        return null;
                }

                InitializeSegment(out var baseline, out var segment);
                date = new DateTime(baseline * 10_000);
                if (segment.NumberOfLiveEntries == 0)
                {
                    // find the prev segment
                    date = date.AddMilliseconds(-1);
                    continue;
                }

                SingleResult last = default;
                if (to == DateTime.MaxValue)
                {
                    last = segment.YieldAllValues(_context, date, includeDead: false).Last();
                    if (last.Timestamp < _from)
                        return null;

                    last.Type = IsRaw ? SingleResultType.Raw : SingleResultType.RolledUp;
                    return last;
                }

                foreach (var item in segment.YieldAllValues(_context, date, includeDead: false))
                {
                    if (item.Timestamp > to)
                        return last;

                    last = item;
                    last.Type = IsRaw ? SingleResultType.Raw : SingleResultType.RolledUp;
                }

                if (last?.Timestamp < _from)
                    return null;

                return last;
            }
        }

        internal IEnumerable<TimeSeriesStorage.SegmentSummary> GetSegmentsSummary()
        {
            if (Init() == false)
                yield return null;

            InitializeSegment(out var baselineMilliseconds, out _currentSegment);

            do
            {
                var baseline = new DateTime(baselineMilliseconds * 10_000, DateTimeKind.Utc);

                if (baseline > _to)
                    yield break;

                if (_offset.HasValue)
                {
                    baseline = DateTime.SpecifyKind(baseline, DateTimeKind.Unspecified).Add(_offset.Value);
                }

                yield return new TimeSeriesStorage.SegmentSummary
                {
                    DocumentId = _documentId,
                    Name = _name,
                    StartTime = baseline,
                    NumberOfEntries = _currentSegment.NumberOfEntries,
                    NumberOfLiveEntries = _currentSegment.NumberOfLiveEntries,
                    ChangeVector = GetCurrentSegmentChangeVector()
                };
            } while (NextSegment(out baselineMilliseconds));
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

                result.Count += _currentSegment.NumberOfLiveEntries;
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
                _token.ThrowIfCancellationRequested();

                var baseline = new DateTime(baselineMilliseconds * 10_000, DateTimeKind.Utc);

                if (baseline > _to)
                    yield break;

                if (_currentSegment.NumberOfValues != _values.Length)
                {
                    _values = new double[_currentSegment.NumberOfValues];
                    _states = new TimestampState[_currentSegment.NumberOfValues];
                }

                if (_offset.HasValue)
                {
                    baseline = DateTime.SpecifyKind(baseline, DateTimeKind.Unspecified).Add(_offset.Value);
                }

                segmentResult.End = _currentSegment.GetLastTimestamp(baseline);
                segmentResult.Start = baseline;
                segmentResult.ChangeVector = GetCurrentSegmentChangeVector();

                if (_currentSegment.NumberOfLiveEntries > 0)
                {
                    if (segmentResult.Start >= _from &&
                        segmentResult.End <= _to &&
                        _currentSegment.InvalidLastValue() == false && // legacy issue RavenDB-15645 
                        _currentSegment.Version.ContainsLastValueDuplicate == false) 
                    {
                        // we can yield the whole segment in one go
                        segmentResult.Summary = _currentSegment;
                        yield return (null, segmentResult);
                    }
                    else
                    {
                        yield return (YieldSegment(baseline), segmentResult);
                    }
                }

                if (NextSegment(out baselineMilliseconds) == false)
                    yield break;
            }
        }

        public IEnumerable<SingleResult> AllValues(bool includeDead = false)
        {
            if (Init() == false)
                yield break;

            InitializeSegment(out var baselineMilliseconds, out _currentSegment);
            while (true)
            {
                _token.ThrowIfCancellationRequested();

                var baseline = new DateTime(baselineMilliseconds * 10_000, DateTimeKind.Utc);

                if (baseline > _to)
                    yield break;

                var openSegment = includeDead ? _currentSegment.NumberOfEntries > 0 : _currentSegment.NumberOfLiveEntries > 0;

                if (openSegment)
                {
                    if (_currentSegment.NumberOfValues > _values.Length)
                    {
                        _values = new double[_currentSegment.NumberOfValues];
                        _states = new TimestampState[_currentSegment.NumberOfValues];
                    }

                    if (_offset.HasValue)
                    {
                        baseline = DateTime.SpecifyKind(baseline, DateTimeKind.Unspecified).Add(_offset.Value);
                    }

                    foreach (var val in YieldSegment(baseline, includeDead))
                    {
                        _token.ThrowIfCancellationRequested();
                        yield return val;
                    }
                }

                if (NextSegment(out baselineMilliseconds) == false)
                    yield break;
            }
        }

        public IEnumerable<SingleResult> YieldSegmentMergeDuplicates(DateTime baseline, bool includeDead = false)
        {
            var shouldBreak = includeDead ? _currentSegment.NumberOfEntries == 0 : _currentSegment.NumberOfLiveEntries == 0;
            if (shouldBreak)
                yield break;

            using var enumerator = _currentSegment.YieldAllValues(_context, _context.Allocator, baseline, includeDead).GetEnumerator();

            var previous = new SingleResult {Values = new double[_currentSegment.NumberOfValues]};
            var current = new SingleResult {Values = new double[_currentSegment.NumberOfValues]};
            var aggregated = new double[_currentSegment.NumberOfValues];

            while (true)
            {
                if (enumerator.MoveNext() == false)
                    yield break;

                if (enumerator.Current.Timestamp > _to)
                    yield break;

                if (enumerator.Current.Timestamp < _from)
                    continue;

                enumerator.Current.CopyTo(previous);

                var isDuplicate = false;
                while (true)
                {
                    if (enumerator.MoveNext())
                    {
                        enumerator.Current.CopyTo(current);
                    }
                    else
                    {
                        if (isDuplicate)
                        {
                            previous.Tag = null;
                            aggregated.CopyTo(previous.Values);
                        }
                        else
                        {
                            _details?.Add(previous);
                        }

                        yield return previous;
                        _details?.Reset();
                        yield break;
                    }

                    if (current.Timestamp != previous.Timestamp)
                    {
                        if (isDuplicate)
                        {
                            previous.Tag = null;
                            aggregated.CopyTo(previous.Values);
                        }
                        else
                        {
                            _details?.Add(previous);
                        }

                        yield return previous;
                        _details?.Reset();

                        if (current.Timestamp > _to)
                            yield break;

                        current.CopyTo(previous);

                        if (isDuplicate)
                        {
                            isDuplicate = false;
                            ResetAggregation(aggregated);
                        }
                        continue;
                    }

                    Aggregate(aggregated, current);

                    if (isDuplicate == false)
                    {
                        Aggregate(aggregated, previous);
                        isDuplicate = true;
                    }
                }
            }
        }

        public class DetailedSingleResult
        {
            private readonly string _databaseChangeVector;
            public Dictionary<string, double[]> Details = new Dictionary<string, double[]>();

            public DetailedSingleResult(string databaseChangeVector)
            {
                _databaseChangeVector = databaseChangeVector;
            }

            public void Add(SingleResult result)
            {
                var key = FetchNodeDetails(result, _databaseChangeVector);

                if (Details.TryGetValue(key, out var values) == false)
                    values = new double[result.Values.Length];

                for (int i = 0; i < result.Values.Length; i++)
                {
                    values[i] += result.Values.Span[i];
                }

                Details[key] = values;
            }

            public void Reset()
            {
                Details.Clear();
            }

            private static string FetchNodeDetails(SingleResult result, string databaseChangeVector)
            {
                var dbId = result.Tag.Substring(7); // extract dbId from tag [tag struct: "TC:XXX-dbId" - where "XXX" can be "INC"/"DEC"] 
                var nodeTag = ChangeVectorUtils.GetNodeTagById(databaseChangeVector, dbId) ?? "?";
                return nodeTag + "-" + dbId;
            }
        }

        private static void ResetAggregation(double[] aggregated)
        {
            for (int i = 0; i < aggregated.Length; i++)
            {
                aggregated[i] = 0;
            }
        }

        private void Aggregate(double[] aggregated, SingleResult result)
        {
            _details?.Add(result);
            for (var index = 0; index < aggregated.Length; index++)
            {
                aggregated[index] += result.Values.Span[index];
            }
        }

        public IEnumerable<SingleResult> YieldSegment(DateTime baseline, bool includeDead = false)
        {
            if(_currentSegment.Version.ContainsDuplicates)
                return YieldSegmentMergeDuplicates(baseline, includeDead);

            return YieldSegmentRaw(baseline, includeDead);
        }

        public IEnumerable<SingleResult> YieldSegmentRaw(DateTime baseline, bool includeDead = false)
        {
            var shouldBreak = includeDead ? _currentSegment.NumberOfEntries == 0 : _currentSegment.NumberOfLiveEntries == 0;
            if (shouldBreak)
                yield break;

            foreach (var result in _currentSegment.YieldAllValues(_context, _context.Allocator, baseline, includeDead))
            {
                if (result.Timestamp > _to)
                    yield break;

                if (result.Timestamp < _from)
                    continue;

                result.Type = IsRaw ? SingleResultType.Raw : SingleResultType.RolledUp;
                
                _details?.Add(result);
                yield return result;
                _details?.Reset();
            }
        }

        public DateTime NextSegmentBaseline()
        {
            if (Init() == false)
                return default;

            if (NextSegment(out var baselineMilliseconds) == false)
                return default;

            return new DateTime(baselineMilliseconds * 10_000);
        }

        internal bool NextSegment(out long baselineMilliseconds)
        {
            byte* key = _tvr.Read((int)TimeSeriesStorage.TimeSeriesTable.TimeSeriesKey, out int keySize);
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
            baselineMilliseconds = ReadBaseline();
            var segmentReadOnlyBuffer = _tvr.Read((int)TimeSeriesStorage.TimeSeriesTable.Segment, out int size);
            readOnlySegment = new TimeSeriesValuesSegment(segmentReadOnlyBuffer, size);
        }

        private long ReadBaseline()
        {
            var key = _tvr.Read((int)TimeSeriesStorage.TimeSeriesTable.TimeSeriesKey, out int keySize);
            return Bits.SwapBytes(*(long*)(key + keySize - sizeof(long)));
        }

        public DateTime ReadBaselineAsDateTime()
        {
            return new DateTime(ReadBaseline() * 10_000);
        }

        internal string GetCurrentSegmentChangeVector()
        {
            return DocumentsStorage.TableValueToChangeVector(_context, (int)TimeSeriesStorage.TimeSeriesTable.ChangeVector, ref _tvr);
        }

        internal (long Etag, string ChangeVector, DateTime Baseline) GetSegmentInfo()
        {
            var changeVector = GetCurrentSegmentChangeVector();
            var etag = DocumentsStorage.TableValueToEtag((int)TimeSeriesStorage.TimeSeriesTable.Etag, ref _tvr);
            var baseline = new DateTime(ReadBaseline() * 10_000);

            return (etag, changeVector, baseline);
        }
    }

    public class TimeSeriesMultiReader : ITimeSeriesReader, IEnumerator<TimeSeriesReader>
    {
        private readonly DocumentsOperationContext _context;
        private TimeSeriesReader _reader;
        private readonly string _docId, _source;
        private readonly string _collection;
        private readonly TimeSpan? _offset;
        private readonly TimeValue _groupBy;
        private readonly CancellationToken _token;
        private readonly DateTime _from, _to;
        private Stack<(DateTime Start, string Name)> _timeseriesStack;

        public bool IsRaw => _reader.IsRaw;

        public TimeSeriesMultiReader(DocumentsOperationContext context, string documentId,
            string source, string collection, DateTime from, DateTime to, TimeSpan? offset, TimeValue groupBy, CancellationToken token)
        {
            if (string.IsNullOrEmpty(source))
                throw new ArgumentNullException(nameof(source));
            _source = source;
            _collection = collection;
            _docId = documentId ?? throw new ArgumentNullException(nameof(documentId));
            _context = context;
            _from = from;
            _to = to;
            _offset = offset;
            _groupBy = groupBy;
            _token = token;

            Initialize();
        }

        private void Initialize()
        {
            _timeseriesStack ??= new Stack<(DateTime Start, string Name)>();
            _timeseriesStack.Clear();

            var raw = _context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage.Stats.GetStats(_context, _docId, _source);
            if (raw.Count > 0)
                _timeseriesStack.Push((raw.Start, _source));

            if (TryGetConfig(out var config))
                return;

            foreach (var policy in config.Policies)
            {
                var name = policy.GetTimeSeriesName(_source);
                var stats = _context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage.Stats.GetStats(_context, _docId, name);
                if (stats.Count == 0)
                    continue;

                if (_timeseriesStack.TryPop(out var previous))
                {
                    if (TryFitCurrentPolicy() == false)
                    {
                        _timeseriesStack.Push(previous);
                        continue;
                    }

                    AlignStartPoints(previous);
                }

                _timeseriesStack.Push((stats.Start, name));

                bool TryFitCurrentPolicy()
                {
                    var foundEarlierStart = previous.Start > stats.Start;
                    var isMultiple = policy.AggregationTime.IsMultiple(_groupBy);

                    if (isMultiple)
                    {
                        previous.Start = stats.End.Add(policy.AggregationTime);
                        return true;
                    }
                    if (foundEarlierStart)
                    {
                        // previous is in higher resolution and covers the requested range
                        if (previous.Start <= _from)
                            return false;

                        // prev start overlap with the first aggregation frame
                        // need to adjust to avoid counting same points twice

                        var x = (int)policy.AggregationTime.TimesInInterval(stats.Start, previous.Start) + 1;
                        previous.Start = stats.Start.Add(x * policy.AggregationTime);

                        var end = stats.End;
                        if (end.Add(policy.AggregationTime) < previous.Start)
                            previous.Start = stats.Start.Add((x - 1) * policy.AggregationTime);

                        return true;
                    }
                    return false; // we prefer the higher resolution and it can't have points newer then prev
                }
            }
        }

        private void AlignStartPoints((DateTime Start, string Name) current)
        {
            while (true)
            {
                if (_timeseriesStack.TryPop(out var prev) == false)
                {
                    _timeseriesStack.Push(current);
                    return;
                }

                if (_timeseriesStack.TryPeek(out var beforePrev))
                {
                    if (beforePrev.Start <= current.Start)
                        continue; // prev is not necessary
                }

                if (prev.Start <= current.Start)
                {
                    prev.Start = current.Start;
                    _timeseriesStack.Push(prev);
                }
                else
                {
                    _timeseriesStack.Push(prev);
                    _timeseriesStack.Push(current);
                }

                return;
            }
        }

        private bool TryGetConfig(out TimeSeriesCollectionConfiguration config)
        {
            var policyRunner = _context.DocumentDatabase.TimeSeriesPolicyRunner;
            config = null;

            return _source.Contains(TimeSeriesConfiguration.TimeSeriesRollupSeparator) ||
                   policyRunner == null ||
                   policyRunner.Configuration.Collections.TryGetValue(_collection, out config) == false ||
                   config.Disabled ||
                   config.Policies.Count == 0;
        }

        public IEnumerable<(IEnumerable<SingleResult> IndividualValues, SegmentResult Segment)> SegmentsOrValues()
        {
            while (MoveNext())
            {
                foreach (var sov in Current.SegmentsOrValues())
                {
                    yield return sov;
                }
            }
        }

        public IEnumerable<SingleResult> AllValues(bool includeDead = false)
        {
            while (MoveNext())
            {
                foreach (var singleResult in Current.AllValues(includeDead))
                {
                    yield return singleResult;
                }
            }
        }

        public bool MoveNext()
        {
            while (true)
            {
                if (_timeseriesStack.TryPop(out var current) == false)
                {
                    _reader = null;
                    return false;
                }

                var previousEnd = _reader?._to.AddMilliseconds(1) ?? _from;
                if (previousEnd == _to)
                    return false;

                if (_timeseriesStack.Count == 0)
                {
                    _reader = new TimeSeriesReader(_context, _docId, current.Name, previousEnd, _to, _offset, _token);
                    return true;
                }

                var next = _timeseriesStack.Peek();
                if (next.Start < previousEnd)
                    continue;

                var to = next.Start.AddMilliseconds(-1);
                if (_to < to)
                    to = _to;

                _reader = new TimeSeriesReader(_context, _docId, current.Name, previousEnd, to, _offset, _token);
                return true;
            }
        }

        public void Reset()
        {
            Initialize();
        }

        public TimeSeriesReader Current => _reader;

        object IEnumerator.Current => Current;

        public void Dispose()
        {
        }
    }
}
