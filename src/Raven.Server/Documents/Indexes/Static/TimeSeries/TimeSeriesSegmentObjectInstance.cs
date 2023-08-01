using System;
using System.Collections.Generic;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime;
using Jint.Runtime.Descriptors;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Server.Documents.Indexes.Static.JavaScript;

namespace Raven.Server.Documents.Indexes.Static.TimeSeries
{
    public sealed class TimeSeriesSegmentObjectInstance : ObjectInstance
    {
        private readonly DynamicTimeSeriesSegment _segment;

        private readonly Dictionary<string, PropertyDescriptor> _properties = new();

        public TimeSeriesSegmentObjectInstance(Engine engine, DynamicTimeSeriesSegment segment) : base(engine)
        {
            ArgumentNullException.ThrowIfNull(segment);
            _segment = segment;
        }

        public override bool Delete(JsValue property)
        {
            throw new NotSupportedException();
        }

        public override PropertyDescriptor GetOwnProperty(JsValue property)
        {
            var key = property.ToString();
            if (_properties.TryGetValue(key, out var value) == false)
                _properties[key] = value = GetPropertyValue(key);

            return value;
        }

        private PropertyDescriptor GetPropertyValue(string property)
        {
            return property switch
            {
                nameof(DynamicTimeSeriesSegment.Entries) => new TimeSeriesSegmentEntriesPropertyDescriptor(Engine, _segment),
                nameof(TimeSeriesSegment.DocumentId) => new PropertyDescriptor(new LazyJsString(_segment._segmentEntry.DocId), writable: false, enumerable: false, configurable: false),
                nameof(DynamicTimeSeriesSegment.Name) => new PropertyDescriptor(new LazyJsString(_segment._segmentEntry.Name), writable: false, enumerable: false, configurable: false),
                nameof(DynamicTimeSeriesSegment.Count) => new PropertyDescriptor(_segment.Count, writable: false, enumerable: false, configurable: false),
                nameof(DynamicTimeSeriesSegment.End) => new PropertyDescriptor(new JsDate(_engine, _segment.End), writable: false, enumerable: false, configurable: false),
                nameof(DynamicTimeSeriesSegment.Start) => new PropertyDescriptor(new JsDate(_engine, _segment.Start), writable: false, enumerable: false, configurable: false),
                _ => PropertyDescriptor.Undefined
            };
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

        private sealed class TimeSeriesSegmentEntriesPropertyDescriptor : PropertyDescriptor
        {
            private readonly JsArray _value;

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

            private static JsArray CreateValue(Engine engine, DynamicTimeSeriesSegment segment)
            {
                var items = new PropertyDescriptor[segment._segmentEntry.Segment.NumberOfLiveEntries];
                var i = 0;
                foreach (DynamicTimeSeriesSegment.DynamicTimeSeriesEntry entry in segment.Entries)
                {
                    items[i] = new TimeSeriesSegmentEntryPropertyDescriptor(engine, entry);
                    i++;
                }

                var jsArray = new JsArray(engine, items);
                return jsArray;
            }
        }

        private sealed class TimeSeriesSegmentEntryPropertyDescriptor : PropertyDescriptor
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
                var value = new JsObject(engine);

                value.FastSetDataProperty(nameof(entry.Tag), entry._entry.Tag is not null ? new LazyJsString(entry._entry.Tag) : DynamicJsNull.ExplicitNull);
                value.FastSetDataProperty(nameof(entry.Timestamp), new JsDate(engine, entry._entry.Timestamp));

                var values = new JsValue[entry._entry.Values.Length];
                for (var i = 0; i < values.Length; i++)
                    values[i] = entry._entry.Values.Span[i];

                var array = new JsArray(engine, values);

                value.FastSetDataProperty(nameof(entry.Value), values[0]);
                value.FastSetDataProperty(nameof(entry.Values), array);

                return value;
            }
        }
    }
}
