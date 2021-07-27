using System;
using System.Collections.Generic;
using V8.Net;
using Raven.Client.Documents.Indexes.TimeSeries;

namespace Raven.Server.Documents.Indexes.Static.TimeSeries
{
    public class TimeSeriesSegmentObjectInstance : PropertiesObjectInstance
    {
        private readonly DynamicTimeSeriesSegment _segment;

        public TimeSeriesSegmentObjectInstance(DynamicTimeSeriesSegment segment) : base()
        {
            _segment = segment ?? throw new ArgumentNullException(nameof(segment));
        }

        public TimeSeriesSegmentObjectInstance() : base()
        {
            assert(false);
        }
        
        public InternalHandle NamedPropertyGetter(V8Engine engine, ref string propertyName)
        {
            if (_properties.TryGetValue(propertyName, out InternalHandle value) == false)
            {
                value = GetPropertyValue(engine, propertyName);
                _properties[propertyName].Set(value);
            }

            return value;
        }

        private InternalHandle GetPropertyValue(V8Engine engine, ref string propertyName)
        {
            InternalHandle jsRes;
            if (propertyName == nameof(DynamicTimeSeriesSegment.Entries))
                return jsRes.Set((V8EngineEx)engine.CreateObjectBinder(_segment.Entries, (V8EngineEx)engine.TypeBinderDynamicTimeSeriesEntries)._);

            if (propertyName == nameof(TimeSeriesSegment.DocumentId))
                return engine.CreateValue(_segment._segmentEntry.DocId.ToString());

            if (propertyName == nameof(DynamicTimeSeriesSegment.Name))
                return engine.CreateValue(_segment._segmentEntry.Name.ToString());

            if (propertyName == nameof(DynamicTimeSeriesSegment.Count))
                return engine.CreateValue(_segment.Count);

            if (propertyName == nameof(DynamicTimeSeriesSegment.End))
                return engine.CreateValue(_engine.Date.Construct(_segment.End));

            if (propertyName == nameof(DynamicTimeSeriesSegment.Start))
                return engine.CreateValue(_engine.Date.Construct(_segment.Start));

            return InternalHandle.Empty;
        }

        public class CustomBinder : PropertiesObjectInstance.CustomBinder<TimeSeriesSegmentObjectInstance>
        {
            public override InternalHandle NamedPropertyGetter(ref string propertyName)
            {
                return _Handle.NamedPropertyGetter(Engine, propertyName);
            }

            public override V8PropertyAttributes? NamedPropertyQuery(ref string propertyName)
            {
                string[] propertyNames = { nameof(DynamicTimeSeriesSegment.Entries), nameof(TimeSeriesSegment.DocumentId), nameof(DynamicTimeSeriesSegment.Name), nameof(DynamicTimeSeriesSegment.Count), nameof(DynamicTimeSeriesSegment.End), nameof(DynamicTimeSeriesSegment.Start) };
                if (propertyNames.IndexOf(propertyName) > -1)
                {
                    return V8PropertyAttributes.Locked | V8PropertyAttributes.DontEnum;
                }
                return null;
            }

            public override InternalHandle NamedPropertyEnumerator()
            {
                string[] propertyNames = { nameof(_Handle.Tag), nameof(_Handle.Timestamp), nameof(_Handle.Value), nameof(_Handle.Values) };

                int arrayLength =  propertyNames.Length;
                var jsItems = propertyNames.Select(x => Engine.CreateValue(x)).ToArray();
                return (V8EngineEx)Engine.CreateArrayWithDisposal(jsItems);
            }
        }
    }

    public class DynamicTimeSeriesEntriesCustomBinder : ObjectBinder<DynamicArray>
    {

        public override InternalHandle IndexedPropertyGetter(int index)
        {
            InternalHandle jsRes;
            if (index < _Handle.Length)
                return jsRes.Set((V8EngineEx)Engine.CreateObjectBinder((DynamicTimeSeriesSegment.DynamicTimeSeriesEntry)_Handle[index])._);

            return InternalHandle.Empty;
        }

        public override V8PropertyAttributes? IndexedPropertyQuery(int index)
        {
            if (index < _Handle.Length)
                return ScriptMemberSecurity.Locked;

            return null;
        }
        public override InternalHandle IndexedPropertyEnumerator()
        {
            int arrayLength =  _Handle.Length;
            var jsItems = Enumerable.Range(0, arrayLength).Select(x => Engine.CreateValue(x)).ToArray();

            return (V8EngineEx)Engine.CreateArrayWithDisposal(jsItems);
        }
    }

    public class DynamicTimeSeriesEntryCustomBinder : ObjectBinder<DynamicTimeSeriesSegment.DynamicTimeSeriesEntry>
    {
        public override InternalHandle NamedPropertyGetter(ref string propertyName)
        {
            if (propertyName == nameof(_Handle.Tag))
                return Engine.CreateValue(_Handle._entry.Tag?.ToString());
            if (propertyName == nameof(_Handle.Timestamp))
                return Engine.CreateValue(_Handle._entry.Timestamp);
            if (propertyName == nameof(_Handle.Value))
                return Engine.CreateValue(_Handle._entry.Values.Span[0]);


            if (propertyName == nameof(_Handle.Values))
            {
                int arrayLength =  _Handle._entry.Values.Length;
                var jsItems = new InternalHandle[arrayLength];
                for (int i = 0; i < arrayLength; ++i)
                {
                    jsItems[i] = Engine.CreateValue(_Handle._entry.Values.Span[i]);
                }

                return (V8EngineEx)Engine.CreateArrayWithDisposal(jsItems);
            }

            return InternalHandle.Empty;
        }

        public override V8PropertyAttributes? NamedPropertyQuery(ref string propertyName)
        {
            string[] propertyNames = { nameof(_Handle.Tag), nameof(_Handle.Timestamp), nameof(_Handle.Value), nameof(_Handle.Values) };
            if (propertyNames.IndexOf(propertyName) > -1)
            {
                return V8PropertyAttributes.Locked;
            }
            return null;
        }

        public override InternalHandle NamedPropertyEnumerator()
        {
            string[] propertyNames = { nameof(_Handle.Tag), nameof(_Handle.Timestamp), nameof(_Handle.Value), nameof(_Handle.Values) };

            int arrayLength =  propertyNames.Length;
            var jsItems = propertyNames.Select(x => Engine.CreateValue(x)).ToArray();
            return (V8EngineEx)Engine.CreateArrayWithDisposal(jsItems);
        }
    }
}
