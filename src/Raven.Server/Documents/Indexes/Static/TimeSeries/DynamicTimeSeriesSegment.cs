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

        public override void Set(object item)
        {
            _item = (TimeSeriesItem)item;
        }

        protected override bool TryGetByName(string name, out object result)
        {
            Debug.Assert(_item != null, "Item cannot be null");

            if (string.Equals("Entries", name))
            {
                var context = CurrentIndexingScope.Current.IndexContext;
                var entries = _item.Segment.YieldAllValues(context, context.Allocator, _item.Baseline);
                var enumerable = new DynamicTimeSeriesEnumerable(entries);

                result = new DynamicArray(enumerable);
                return true;
            }

            if (string.Equals("DocumentId", name))
            {
                result = TypeConverter.ToDynamicType(_item.DocId);
                return true;
            }

            if (string.Equals(nameof(_item.Baseline), name))
            {
                result = TypeConverter.ToDynamicType(_item.Baseline);
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

                public DynamicTimeSeriesEntry(TimeSeriesStorage.Reader.SingleResult entry)
                {
                    _entry = entry;
                }

                public override void Set(object item)
                {
                    _entry = (TimeSeriesStorage.Reader.SingleResult)item;
                }

                protected override bool TryGetByName(string name, out object result)
                {
                    Debug.Assert(_entry != null, "Entry cannot be null");

                    if (string.Equals(nameof(_entry.Tag), name))
                    {
                        result = TypeConverter.ToDynamicType(_entry.Tag);
                        return true;
                    }

                    if (string.Equals(nameof(_entry.TimeStamp), name))
                    {
                        result = TypeConverter.ToDynamicType(_entry.TimeStamp);
                        return true;
                    }

                    if (string.Equals(nameof(_entry.Values), name))
                    {
                        // TODO [ppekrol] can we do better here? Implement dedicated DynamicArray for this?
                        result = new DynamicArray(_entry.Values.ToArray());
                        return true;
                    }

                    result = DynamicNullObject.Null;
                    return true;
                }
            }
        }
    }
}
