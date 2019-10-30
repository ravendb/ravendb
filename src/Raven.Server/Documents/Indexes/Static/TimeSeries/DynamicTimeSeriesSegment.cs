using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Indexes.Static.TimeSeries
{
    public class DynamicTimeSeriesSegment : AbstractDynamicObject
    {
        private TimeSeriesItem _item;
        private DynamicArray _entries;

        public override dynamic GetId()
        {
            if (_item == null)
                return DynamicNullObject.Null;

            Debug.Assert(_item.Key != null, "_item.Key != null");
            Debug.Assert(nameof(Baseline) == nameof(_item.Baseline), "nameof(Baseline) == nameof(_item.Baseline)");

            return _item.Key;
        }

        public override void Set(object item)
        {
            _item = (TimeSeriesItem)item;
            _entries = null;
        }

        public DynamicArray Entries
        {
            get
            {
                if (_entries == null)
                {
                    var context = CurrentIndexingScope.Current.IndexContext;
                    var entries = _item.Segment.YieldAllValues(context, context.Allocator, _item.Baseline);
                    var enumerable = new DynamicTimeSeriesEnumerable(entries);

                    _entries = new DynamicArray(enumerable);
                }

                return _entries;
            }
        }

        public dynamic Baseline => TypeConverter.ToDynamicType(_item.Baseline);

        protected override bool TryGetByName(string name, out object result)
        {
            Debug.Assert(_item != null, "Item cannot be null");

            if (string.Equals("DocumentId", name)) // TODO arek - https://github.com/ravendb/ravendb/pull/9875/files#r346221961
            {
                result = TypeConverter.ToDynamicType(_item.DocId);
                return true;
            }

            if (string.Equals(nameof(Entries), name))
            {
                result = Entries;
                return true;
            }

            if (string.Equals(nameof(Baseline), name))
            {
                result = Baseline;
                return true;
            }

            result = DynamicNullObject.Null;
            return true;
        }

        private class DynamicTimeSeriesEnumerable : IEnumerable<object>
        {
            private readonly IEnumerable<TimeSeriesStorage.Reader.SingleResult> _inner;

            public DynamicTimeSeriesEnumerable(IEnumerable<TimeSeriesStorage.Reader.SingleResult> inner)
            {
                _inner = inner;
            }

            public IEnumerator<object> GetEnumerator()
            {
                return new Enumerator(_inner.GetEnumerator());
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            private class Enumerator : IEnumerator<object>
            {
                private IEnumerator<TimeSeriesStorage.Reader.SingleResult> _inner;

                public Enumerator(IEnumerator<TimeSeriesStorage.Reader.SingleResult> inner)
                {
                    _inner = inner;
                }

                public object Current { get; private set; }

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
            }

            private class DynamicTimeSeriesEntry : AbstractDynamicObject
            {
                private TimeSeriesStorage.Reader.SingleResult _entry;
                private DynamicArray _values;

                public DynamicTimeSeriesEntry(TimeSeriesStorage.Reader.SingleResult entry)
                {
                    Debug.Assert(nameof(Values) == nameof(entry.Values), "nameof(Values) == nameof(entry.Values)");
                    Debug.Assert(nameof(TimeStamp) == nameof(_entry.TimeStamp), "nameof(TimeStamp) == nameof(_entry.TimeStamp");
                    Debug.Assert(nameof(Tag) == nameof(_entry.Tag),"nameof(Tag) == nameof(_entry.Tag)");

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

                public dynamic TimeStamp => TypeConverter.ToDynamicType(_entry.TimeStamp);

                public dynamic Tag => TypeConverter.ToDynamicType(_entry.Tag);

                protected override bool TryGetByName(string name, out object result)
                {
                    Debug.Assert(_entry != null, "Entry cannot be null");

                    if (string.Equals(nameof(_entry.Tag), name))
                    {
                        result = Tag;
                        return true;
                    }

                    if (string.Equals(nameof(_entry.TimeStamp), name))
                    {
                        result = TimeStamp;
                        return true;
                    }

                    if (string.Equals(nameof(_entry.Values), name))
                    {
                        result = Values;
                        return true;
                    }

                    result = DynamicNullObject.Null;
                    return true;
                }
            }
        }
    }
}
