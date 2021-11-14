using System;
using System.Collections.Generic;
using Jint;
using Jint.Native;
using Jint.Native.Array;
using Jint.Native.Object;
using Jint.Runtime;
using Jint.Runtime.Descriptors;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.Jint;

namespace Raven.Server.Documents.Indexes.Static.TimeSeries.Jint
{
    public class TimeSeriesSegmentObjectInstanceJint : ObjectInstance, IObjectInstance
    {
        private JintEngineEx _engineEx;
        private Engine _engine;

        private readonly DynamicTimeSeriesSegment _segment;

        private Dictionary<JsValue, PropertyDescriptor> _properties = new Dictionary<JsValue, PropertyDescriptor>();

        public IJsEngineHandle EngineHandle => _engineEx;

        public JsHandle CreateJsHandle(bool keepAlive = false)
        {
            return new JsHandle(this);
        }

        public void Dispose()
        {}

        public TimeSeriesSegmentObjectInstanceJint(JintEngineEx engineEx, DynamicTimeSeriesSegment segment) 
            : base(engineEx)
        {
            _engineEx = engineEx;
            _engine = _engineEx;
                
            _segment = segment ?? throw new ArgumentNullException(nameof(segment));

            SetPrototypeOf(_engine.Realm.Intrinsics.Object.PrototypeObject);
        }

        public override bool Delete(JsValue property)
        {
            throw new NotSupportedException();
        }

        public override PropertyDescriptor GetOwnProperty(JsValue property)
        {
            if (_properties.TryGetValue(property, out var value) == false)
                _properties[property] = value = GetPropertyValue(property);

            return value;
        }

        private PropertyDescriptor GetPropertyValue(JsValue property)
        {
            if (property == nameof(DynamicTimeSeriesSegment.Entries))
                return new TimeSeriesSegmentEntriesPropertyDescriptor(Engine, _segment);

            if (property == nameof(TimeSeriesSegment.DocumentId))
                return new PropertyDescriptor(_segment._segmentEntry.DocId.ToString(), writable: false, enumerable: false, configurable: false);

            if (property == nameof(DynamicTimeSeriesSegment.Name))
                return new PropertyDescriptor(_segment._segmentEntry.Name.ToString(), writable: false, enumerable: false, configurable: false);

            if (property == nameof(DynamicTimeSeriesSegment.Count))
                return new PropertyDescriptor(_segment.Count, writable: false, enumerable: false, configurable: false);

            if (property == nameof(DynamicTimeSeriesSegment.End))
                return new PropertyDescriptor(_engine.Realm.Intrinsics.Date.Construct(_segment.End), writable: false, enumerable: false, configurable: false);

            if (property == nameof(DynamicTimeSeriesSegment.Start))
                return new PropertyDescriptor(_engine.Realm.Intrinsics.Date.Construct(_segment.Start), writable: false, enumerable: false, configurable: false);

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
                var items = new PropertyDescriptor[segment._segmentEntry.Segment.NumberOfLiveEntries];
                var i = 0;
                foreach (DynamicTimeSeriesSegment.DynamicTimeSeriesEntry entry in segment.Entries)
                {
                    items[i] = new TimeSeriesSegmentEntryPropertyDescriptor(engine, entry);
                    i++;
                }

                var jsArray = new ArrayInstance(engine, items);
                jsArray.SetPrototypeOf(engine.Realm.Intrinsics.Array.PrototypeObject);

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
                value.Set(nameof(entry.Timestamp), engine.Realm.Intrinsics.Date.Construct(entry._entry.Timestamp));

                var values = new JsValue[entry._entry.Values.Length];
                for (var i = 0; i < values.Length; i++)
                    values[i] = entry._entry.Values.Span[i];

                var array = engine.Realm.Intrinsics.Array.Construct(Arguments.Empty);
                engine.Realm.Intrinsics.Array.PrototypeObject.Push(array, values);

                value.Set(nameof(entry.Value), values[0]);
                value.Set(nameof(entry.Values), array);

                value.SetPrototypeOf(engine.Realm.Intrinsics.Object.PrototypeObject);

                return value;
            }
        }
    }
}
