using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static.TimeSeries
{
    public class DynamicTimeSeriesSegment : AbstractDynamicObject
    {
        private TimeSeriesSegmentEntry _segmentEntry;
        private DynamicArray _entries;

        public override dynamic GetId()
        {
            if (_segmentEntry == null)
                return DynamicNullObject.Null;

            Debug.Assert(_segmentEntry.Key != null, "_segmentEntry.Key != null");
            Debug.Assert(nameof(Baseline) == nameof(_segmentEntry.Baseline), "nameof(Baseline) == nameof(_segmentEntry.Baseline)");

            return _segmentEntry.Key;
        }

        public override void Set(object item)
        {
            _segmentEntry = (TimeSeriesSegmentEntry)item;
            _entries = null;
        }

        public DynamicArray Entries
        {
            get
            {
                if (_entries == null)
                {
                    var context = CurrentIndexingScope.Current.IndexContext;
                    var entries = _segmentEntry.Segment.YieldAllValues(context, context.Allocator, _segmentEntry.Baseline, includeDead: false);
                    var enumerable = new DynamicTimeSeriesEnumerable(entries);

                    _entries = new DynamicArray(enumerable);
                }

                return _entries;
            }
        }

        public LazyStringValue Name
        {
            get
            {
                return _segmentEntry.Name;
            }
        }

        public dynamic Baseline => TypeConverter.ToDynamicType(_segmentEntry.Baseline);

        protected override bool TryGetByName(string name, out object result)
        {
            Debug.Assert(_segmentEntry != null, "Item cannot be null");

            if (string.Equals(nameof(TimeSeriesSegment.DocumentId), name))
            {
                result = TypeConverter.ToDynamicType(_segmentEntry.DocId);
                return true;
            }

            result = DynamicNullObject.Null;
            return true;
        }

        private class DynamicTimeSeriesEnumerable : IEnumerable<DynamicTimeSeriesEntry>
        {
            private readonly IEnumerable<TimeSeriesStorage.Reader.SingleResult> _inner;

            public DynamicTimeSeriesEnumerable(IEnumerable<TimeSeriesStorage.Reader.SingleResult> inner)
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
                private IEnumerator<TimeSeriesStorage.Reader.SingleResult> _inner;

                public Enumerator(IEnumerator<TimeSeriesStorage.Reader.SingleResult> inner)
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
            private TimeSeriesStorage.Reader.SingleResult _entry;
            private DynamicArray _values;

            public DynamicTimeSeriesEntry(TimeSeriesStorage.Reader.SingleResult entry)
            {
                Debug.Assert(nameof(Values) == nameof(entry.Values), "nameof(Values) == nameof(entry.Values)");
                Debug.Assert(nameof(Timestamp) == nameof(_entry.Timestamp), "nameof(Timestamp) == nameof(_segmentEntry.Timestamp");
                Debug.Assert(nameof(Tag) == nameof(_entry.Tag), "nameof(Tag) == nameof(_segmentEntry.Tag)");

                _entry = entry;
            }

            public override dynamic GetId()
            {
                throw new NotSupportedException();
            }

            public override void Set(object item)
            {
                _entry = (TimeSeriesStorage.Reader.SingleResult)item;
                _values = null;
            }

            public dynamic Values
            {
                get
                {
                    return _values ??= new DynamicArray(_entry.Values.ToArray());
                }
            }

            public dynamic Value
            {
                get
                {
                    if (_values == null)
                        _values = new DynamicArray(_entry.Values.ToArray());

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
        }
    }
}
