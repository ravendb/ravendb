using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Indexes.Static.TimeSeries
{
    public class DynamicTimeSeriesSegment : AbstractDynamicObject
    {
        internal TimeSeriesSegmentEntry _segmentEntry;
        private DynamicArray _entries;
        private DynamicArray _min;
        private DynamicArray _max;
        private DynamicArray _sum;
        private TimeSeriesSegmentSummary? _summary;
        private string _name;

        public override dynamic GetId()
        {
            if (_segmentEntry == null)
                return DynamicNullObject.Null;

            Debug.Assert(_segmentEntry.Key != null, "_segmentEntry.Key != null");
            Debug.Assert(nameof(Start) == nameof(_segmentEntry.Start), "nameof(Start) == nameof(_segmentEntry.Start)");

            return _segmentEntry.Key;
        }

        public override bool Set(object item)
        {
            _segmentEntry = (TimeSeriesSegmentEntry)item;
            _entries = null;
            _summary = null;
            _min = null;
            _max = null;
            _sum = null;
            _name = null;
            return true;
        }

        public string Name
        {
            get
            {
                AssertSegment();

                if (_name == null)
                {
                    var context = CurrentIndexingScope.Current.QueryContext;
                    var ts = context.Documents.DocumentDatabase.DocumentsStorage.TimeSeriesStorage;
                    _name = ts.GetOriginalName(context.Documents, _segmentEntry.DocId, _segmentEntry.Name);
                }

                return _name;
            }
        }

        public DateTime Start
        {
            get
            {
                AssertSegment();

                return _segmentEntry.Start;
            }
        }

        public DateTime End
        {
            get
            {
                AssertSegment();

                return _segmentEntry.Segment.GetLastTimestamp(_segmentEntry.Start);
            }
        }

        public DynamicArray Min
        {
            get
            {
                AssertSegment();

                return _min ??= new DynamicArray(Summary.Min);
            }
        }

        public DynamicArray Max
        {
            get
            {
                AssertSegment();

                return _max ??= new DynamicArray(Summary.Max);
            }
        }

        public DynamicArray Sum
        {
            get
            {
                AssertSegment();

                return _sum ??= new DynamicArray(Summary.Sum);
            }
        }

        public int Count
        {
            get
            {
                AssertSegment();

                return Summary.Count;
            }
        }

        public DynamicArray Entries
        {
            get
            {
                AssertSegment();

                if (_entries == null)
                {
                    var context = CurrentIndexingScope.Current.IndexContext;
                    var entries = _segmentEntry.Segment.YieldAllValues(context, context.Allocator, _segmentEntry.Start, includeDead: false);
                    var enumerable = new DynamicTimeSeriesEnumerable(entries);

                    _entries = new DynamicArray(enumerable);
                }

                return _entries;
            }
        }

        protected override bool TryGetByName(string name, out object result)
        {
            AssertSegment();

            if (string.Equals(nameof(TimeSeriesSegment.DocumentId), name))
            {
                result = TypeConverter.ToDynamicType(_segmentEntry.DocId);
                return true;
            }

            result = DynamicNullObject.Null;
            return true;
        }

        private TimeSeriesSegmentSummary Summary => _summary ?? (_summary = _segmentEntry.Segment.GetSummary()).Value;

        [Conditional("DEBUG")]
        private void AssertSegment()
        {
            if (_segmentEntry == null)
                throw new ArgumentNullException(nameof(_segmentEntry));

            if (_segmentEntry.Segment.NumberOfLiveEntries == 0)
                throw new InvalidOperationException("Indexing empty time series segment. Should not happen.");
        }

        private class DynamicTimeSeriesEnumerable : IEnumerable<DynamicTimeSeriesEntry>
        {
            private readonly IEnumerable<SingleResult> _inner;

            public DynamicTimeSeriesEnumerable(IEnumerable<SingleResult> inner)
            {
                _inner = inner;
            }

            public IEnumerator<DynamicTimeSeriesEntry> GetEnumerator()
            {
                return new Enumerator(_inner.GetEnumerator());
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            private class Enumerator : IEnumerator<DynamicTimeSeriesEntry>
            {
                private readonly IEnumerator<SingleResult> _inner;

                public Enumerator(IEnumerator<SingleResult> inner)
                {
                    _inner = inner;
                }

                public DynamicTimeSeriesEntry Current { get; private set; }

                public void Dispose()
                {
                }

                public bool MoveNext()
                {
                    if (_inner.MoveNext() == false)
                        return false;

                    Current = new DynamicTimeSeriesEntry(_inner.Current);
                    return true;
                }

                public void Reset()
                {
                    throw new NotSupportedException();
                }

                object IEnumerator.Current { get; }
            }
        }

        public class DynamicTimeSeriesEntry : AbstractDynamicObject
        {
            internal SingleResult _entry;
            private DynamicArray _values;

            public DynamicTimeSeriesEntry(SingleResult entry)
            {
                Debug.Assert(nameof(Values) == nameof(entry.Values), "nameof(Values) == nameof(entry.Values)");
                Debug.Assert(nameof(Timestamp) == nameof(_entry.Timestamp), "nameof(Timestamp) == nameof(_segmentEntry.Timestamp");
                Debug.Assert(nameof(Tag) == nameof(_entry.Tag), "nameof(Tag) == nameof(_segmentEntry.Tag)");

                _entry = entry;
                _values = CreateValues(_entry);
            }

            public override dynamic GetId()
            {
                throw new NotSupportedException();
            }

            public override bool Set(object item)
            {
                _entry = (SingleResult)item;
                _values = CreateValues(_entry);
                return true;
            }

            public dynamic Values
            {
                get
                {
                    return _values;
                }
            }

            public dynamic Value
            {
                get
                {
                    return _values.Get(0);
                }
            }

            public dynamic Timestamp => TypeConverter.ToDynamicType(_entry.Timestamp);

            public dynamic Tag => TypeConverter.ToDynamicType(_entry.Tag);

            protected override bool TryGetByName(string name, out object result)
            {
                Debug.Assert(_entry != null, "Entry cannot be null");

                result = DynamicNullObject.Null;
                return true;
            }

            private static DynamicArray CreateValues(SingleResult entry)
            {
                return new DynamicArray(entry.Values.ToArray());
            }
        }
    }
}
