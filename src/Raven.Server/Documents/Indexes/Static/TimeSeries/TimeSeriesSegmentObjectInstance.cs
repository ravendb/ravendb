using System;
using System.Collections.Generic;
using Jint;
using Jint.Native;
using Jint.Native.Array;
using Jint.Native.Object;
using Jint.Runtime;
using Jint.Runtime.Descriptors;
using Raven.Client.Documents.Indexes.TimeSeries;

namespace Raven.Server.Documents.Indexes.Static.TimeSeries
{
    public class TimeSeriesSegmentObjectInstance : ObjectInstance
    {
        private readonly DynamicTimeSeriesSegment _segment;

        private TimeSeriesSegmentEntriesPropertyDescriptor _entries;

        private PropertyDescriptor _documentId;

        public TimeSeriesSegmentObjectInstance(Engine engine, DynamicTimeSeriesSegment segment) : base(engine)
        {
            _segment = segment ?? throw new ArgumentNullException(nameof(segment));

            SetPrototypeOf(engine.Object.PrototypeObject);
        }

        public override bool Delete(JsValue property)
        {
            throw new NotSupportedException();
        }

        public override PropertyDescriptor GetOwnProperty(JsValue property)
        {
            if (property == nameof(DynamicTimeSeriesSegment.Entries))
            {
                if (_entries == null)
                    _entries = new TimeSeriesSegmentEntriesPropertyDescriptor(Engine, _segment);

                return _entries;
            }
            else if (property == nameof(TimeSeriesSegment.DocumentId))
            {
                if (_documentId == null)
                    _documentId = new PropertyDescriptor(_segment._segmentEntry.DocId.ToString(), writable: false, enumerable: false, configurable: false);

                return _documentId;
            }

            return PropertyDescriptor.Undefined;
        }

        public override bool Set(JsValue property, JsValue value, JsValue receiver)
        {
            throw new NotSupportedException();
        }

        public override IEnumerable<KeyValuePair<JsValue, PropertyDescriptor>> GetOwnProperties()
        {
            throw new NotSupportedException();
        }

        public override List<JsValue> GetOwnPropertyKeys(Types types = Types.String | Types.Symbol)
        {
            throw new NotSupportedException();
        }

        private class TimeSeriesSegmentEntriesPropertyDescriptor : PropertyDescriptor
        {
            private readonly ArrayInstance _value;

            public TimeSeriesSegmentEntriesPropertyDescriptor(Engine engine, DynamicTimeSeriesSegment segment)
                : base(PropertyFlag.CustomJsValue | PropertyFlag.Writable | PropertyFlag.WritableSet | PropertyFlag.Enumerable | PropertyFlag.EnumerableSet)
            {
                _value = CreateValue(engine, segment);
            }

            public override JsValue Get => CustomValue;

            public override JsValue Set => throw new NotSupportedException();

            protected override JsValue CustomValue
            {
                get => _value;
                set
                {
                    throw new NotSupportedException();
                }
            }

            private static ArrayInstance CreateValue(Engine engine, DynamicTimeSeriesSegment segment)
            {
                var items = new List<PropertyDescriptor>();
                foreach (DynamicTimeSeriesSegment.DynamicTimeSeriesEntry entry in segment.Entries)
                {
                    items.Add(new TimeSeriesSegmentEntryPropertyDescriptor(engine, entry));
                }

                var jsArray = new ArrayInstance(engine, items.ToArray()); // TODO ppekrol - avoid ToArray
                jsArray.SetPrototypeOf(engine.Array.PrototypeObject);

                return jsArray;
            }
        }

        private class TimeSeriesSegmentEntryPropertyDescriptor : PropertyDescriptor
        {
            private readonly ObjectInstance _value;

            public TimeSeriesSegmentEntryPropertyDescriptor(Engine engine, DynamicTimeSeriesSegment.DynamicTimeSeriesEntry entry)
                : base(PropertyFlag.CustomJsValue | PropertyFlag.Writable | PropertyFlag.WritableSet | PropertyFlag.Enumerable | PropertyFlag.EnumerableSet)
            {
                _value = CreateValue(engine, entry);
            }

            public override JsValue Get => CustomValue;

            public override JsValue Set => throw new NotSupportedException();

            protected override JsValue CustomValue
            {
                get => _value;
                set
                {
                    throw new NotSupportedException();
                }
            }

            private static ObjectInstance CreateValue(Engine engine, DynamicTimeSeriesSegment.DynamicTimeSeriesEntry entry)
            {
                var value = new ObjectInstance(engine);

                value.Set(nameof(entry.Tag), entry._entry.Tag?.ToString());
                value.Set(nameof(entry.Timestamp), engine.Date.Construct(entry._entry.Timestamp));

                var values = new JsValue[entry._entry.Values.Length];
                for (var i = 0; i < values.Length; i++)
                    values[i] = entry._entry.Values.Span[i];

                var array = engine.Array.Construct(Arguments.Empty);
                engine.Array.PrototypeObject.Push(array, values);

                value.Set(nameof(entry.Value), values[0]);
                value.Set(nameof(entry.Values), array);

                value.SetPrototypeOf(engine.Object.PrototypeObject);

                return value;
            }
        }
    }
}
