﻿using System;
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

        public TimeSeriesSegmentObjectInstance(DynamicTimeSeriesSegment segment, JavaScriptUtils javaScriptUtils = null) : base(javaScriptUtils)
        {
            _segment = segment ?? throw new ArgumentNullException(nameof(segment));
        }

        public InternalHandle NamedPropertyGetter(V8Engine engine, ref string propertyName)
        {
            if (_properties.TryGetValue(propertyName, out InternalHandle value) == false)
            {
                value = GetPropertyValue(engine, ref propertyName);
                if (value.IsEmpty == false)
                    _properties.Add(propertyName, value);
            }

            return value;
        }

        private InternalHandle GetPropertyValue(V8Engine engine, ref string propertyName)
        {
            if (propertyName == nameof(DynamicTimeSeriesSegment.Entries))
                return new InternalHandle(((V8EngineEx)engine).CreateObjectBinder<DynamicTimeSeriesEntriesCustomBinder>(_segment.Entries, JavaScriptUtils?.TypeBinderDynamicTimeSeriesEntries)._);

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

            public override InternalHandle NamedPropertyGetter(ref string propertyName)
            {
                return objCLR.NamedPropertyGetter(Engine, ref propertyName);
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
                string[] propertyNames = { nameof(objCLR.Tag), nameof(objCLR.Timestamp), nameof(objCLR.Value), nameof(objCLR.Values) };

                int arrayLength =  propertyNames.Length;
                var jsItems = propertyNames.Select(x => Engine.CreateValue(x)).ToArray();
                return ((V8EngineEx)Engine).CreateArrayWithDisposal(jsItems);
            }
        }
    }

    public class DynamicTimeSeriesEntriesCustomBinder : ObjectBinderEx<DynamicArray>
    {

        public DynamicTimeSeriesEntriesCustomBinder() : base()
        {}

        public override InternalHandle IndexedPropertyGetter(int index)
        {
            InternalHandle jsRes;
            if (index < objCLR.Count()) 
            {
                object elem = objCLR.Get(index);
                return jsRes.Set(((V8EngineEx)Engine).CreateObjectBinder<DynamicTimeSeriesEntryCustomBinder>(elem)._);
            }

            return InternalHandle.Empty;
        }

        public override V8PropertyAttributes? IndexedPropertyQuery(int index)
        {
            if (index < objCLR.Count())
                return V8PropertyAttributes.Locked;

            return null;
        }
        public override InternalHandle IndexedPropertyEnumerator()
        {
            int arrayLength =  objCLR.Count();
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

            if (propertyName == nameof(objCLR.Tag))
                return Engine.CreateValue(objCLR._entry.Tag?.ToString());
            if (propertyName == nameof(objCLR.Timestamp))
                return Engine.CreateValue(objCLR._entry.Timestamp);
            if (propertyName == nameof(objCLR.Value))
                return Engine.CreateValue(objCLR._entry.Values.Span[0]);


            if (propertyName == nameof(objCLR.Values))
            {
                int arrayLength =  objCLR._entry.Values.Length;
                var jsItems = new InternalHandle[arrayLength];
                for (int i = 0; i < arrayLength; ++i)
                {
                    jsItems[i] = Engine.CreateValue(objCLR._entry.Values.Span[i]);
                }

                return ((V8EngineEx)Engine).CreateArrayWithDisposal(jsItems);
            }

            return InternalHandle.Empty;
        }

        public override V8PropertyAttributes? NamedPropertyQuery(ref string propertyName)
        {
            string[] propertyNames = { nameof(objCLR.Tag), nameof(objCLR.Timestamp), nameof(objCLR.Value), nameof(objCLR.Values) };
            if (Array.IndexOf(propertyNames, propertyName) > -1)
            {
                return V8PropertyAttributes.Locked;
            }
            return null;
        }

        public override InternalHandle NamedPropertyEnumerator()
        {
            string[] propertyNames = { nameof(objCLR.Tag), nameof(objCLR.Timestamp), nameof(objCLR.Value), nameof(objCLR.Values) };

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
