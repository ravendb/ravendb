using System;
using System.Linq;
using System.Collections.Generic;
using V8.Net;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Server.Documents.Indexes.Static.JavaScript;
using Raven.Server.Documents.Patch;

namespace Raven.Server.Documents.Indexes.Static.TimeSeries
{
    public class TimeSeriesSegmentObjectInstance : ObjectInstanceBase
    {
        private readonly DynamicTimeSeriesSegment _segment;

        public DynamicTimeSeriesSegment Segment {
            get {return _segment;}
        }

        public TimeSeriesSegmentObjectInstance(DynamicTimeSeriesSegment segment) : base()
        {
            _segment = segment ?? throw new ArgumentNullException(nameof(segment));
        }

        public static InternalHandle CreateObjectBinder(V8EngineEx engine, TimeSeriesSegmentObjectInstance bo) {
            return engine.CreateObjectBinder<TimeSeriesSegmentObjectInstance.CustomBinder>(bo, engine.TypeBinderTimeSeriesSegmentObjectInstance);
        }

        public override InternalHandle NamedPropertyGetter(V8EngineEx engine, ref string propertyName)
        {
            if (_properties.TryGetValue(propertyName, out InternalHandle jsValue) == false)
            {
                jsValue = GetPropertyValue(engine, ref propertyName);
                if (jsValue.IsEmpty == false)
                    _properties.Add(propertyName, jsValue);
            }

            return jsValue;
        }

        private InternalHandle GetPropertyValue(V8EngineEx engine, ref string propertyName)
        {
            if (propertyName == nameof(DynamicTimeSeriesSegment.Entries))
                return engine.CreateObjectBinder<DynamicTimeSeriesEntriesCustomBinder>(_segment.Entries, engine.TypeBinderDynamicTimeSeriesEntries);

            if (propertyName == nameof(TimeSeriesSegment.DocumentId))
                return engine.CreateValue(_segment._segmentEntry.DocId.ToString());

            if (propertyName == nameof(DynamicTimeSeriesSegment.Name))
                return engine.CreateValue(_segment._segmentEntry.Name.ToString());

            if (propertyName == nameof(DynamicTimeSeriesSegment.Count))
                return engine.CreateValue(_segment.Count);

            if (propertyName == nameof(DynamicTimeSeriesSegment.End))
                return engine.CreateValue(_segment.End);

            if (propertyName == nameof(DynamicTimeSeriesSegment.Start))
                return engine.CreateValue(_segment.Start);

            return InternalHandle.Empty;
        }

        public class CustomBinder : ObjectInstanceBase.CustomBinder<TimeSeriesSegmentObjectInstance>
        {
            public CustomBinder() : base()
            {}

        }
    }

    public class DynamicTimeSeriesEntriesCustomBinder : ObjectBinderEx<DynamicArray>
    {

        public DynamicTimeSeriesEntriesCustomBinder() : base()
        {}

        public override InternalHandle IndexedPropertyGetter(int index)
        {
            InternalHandle jsRes = InternalHandle.Empty;
            if (index < ObjCLR.Count()) 
            {
                object elem = ObjCLR.Get(index);
                return ((V8EngineEx)Engine).CreateObjectBinder<DynamicTimeSeriesEntryCustomBinder>(elem);
            }

            return InternalHandle.Empty;
        }

        public override V8PropertyAttributes? IndexedPropertyQuery(int index)
        {
            if (index < ObjCLR.Count())
                return V8PropertyAttributes.Locked;

            return null;
        }
        public override InternalHandle IndexedPropertyEnumerator()
        {
            int arrayLength = ObjCLR.Count();
            var jsItems = Enumerable.Range(0, arrayLength).Select(x => Engine.CreateValue(x)).ToArray();

            return ((V8EngineEx)Engine).CreateArrayWithDisposal(jsItems);
        }
    }

    public class DynamicTimeSeriesEntryCustomBinder : ObjectBinderEx<DynamicTimeSeriesSegment.DynamicTimeSeriesEntry>
    {
        public DynamicTimeSeriesEntryCustomBinder() : base()
        {}

        public override InternalHandle NamedPropertyGetter(ref string propertyName)
        {
            var ObjCLR = Object as DynamicTimeSeriesSegment.DynamicTimeSeriesEntry;

            if (propertyName == nameof(ObjCLR.Tag))
                return Engine.CreateValue(ObjCLR._entry.Tag?.ToString());
            if (propertyName == nameof(ObjCLR.Timestamp))
                return Engine.CreateValue(ObjCLR._entry.Timestamp);
            if (propertyName == nameof(ObjCLR.Value))
                return Engine.CreateValue(ObjCLR._entry.Values.Span[0]);


            if (propertyName == nameof(ObjCLR.Values))
            {
                int arrayLength =  ObjCLR._entry.Values.Length;
                var jsItems = new InternalHandle[arrayLength];
                for (int i = 0; i < arrayLength; ++i)
                {
                    jsItems[i] = Engine.CreateValue(ObjCLR._entry.Values.Span[i]);
                }

                return ((V8EngineEx)Engine).CreateArrayWithDisposal(jsItems);
            }

            return InternalHandle.Empty;
        }

        public override V8PropertyAttributes? NamedPropertyQuery(ref string propertyName)
        {
            string[] propertyNames = { nameof(ObjCLR.Tag), nameof(ObjCLR.Timestamp), nameof(ObjCLR.Value), nameof(ObjCLR.Values) };
            if (Array.IndexOf(propertyNames, propertyName) > -1)
            {
                return V8PropertyAttributes.Locked;
            }
            return null;
        }

        public override InternalHandle NamedPropertyEnumerator()
        {
            string[] propertyNames = { nameof(ObjCLR.Tag), nameof(ObjCLR.Timestamp), nameof(ObjCLR.Value), nameof(ObjCLR.Values) };

            int arrayLength =  propertyNames.Length;
            var jsItems = new InternalHandle[arrayLength];
            for (int i = 0; i < arrayLength; ++i)
            {
                jsItems[i] = Engine.CreateValue(propertyNames[i]);
            }
            return ((V8EngineEx)Engine).CreateArrayWithDisposal(jsItems);
        }
    }
}
