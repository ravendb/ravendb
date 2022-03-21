using System;
using System.Linq;
using System.Collections.Generic;
using V8.Net;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Server.Documents.Indexes.Static.JavaScript.V8;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.V8;

namespace Raven.Server.Documents.Indexes.Static.TimeSeries.V8
{
    public class TimeSeriesSegmentObjectInstanceV8 : ObjectInstanceBaseV8
    {
        private readonly DynamicTimeSeriesSegment _segment;

        public DynamicTimeSeriesSegment Segment
        {
            get {return _segment;}
        }

        public TimeSeriesSegmentObjectInstanceV8(V8EngineEx engineEx, DynamicTimeSeriesSegment segment) 
            : base(engineEx, false)
        {
            _segment = segment ?? throw new ArgumentNullException(nameof(segment));
        }

        public override InternalHandle CreateObjectBinder(bool keepAlive = false)
        {
            var jsBinder =  _engine.CreateObjectBinder<TimeSeriesSegmentObjectInstanceV8.CustomBinder>(this, EngineEx.Context.TypeBinderTimeSeriesSegmentObjectInstance(), keepAlive: keepAlive);
            var binder = (ObjectBinder)jsBinder.Object;
            binder.ShouldDisposeBoundObject = true;
            return jsBinder;
        }

        public override InternalHandle NamedPropertyGetterOnce(V8EngineEx engineEx, ref string propertyName)
        {
            var engine = (V8Engine)engineEx;
            if (propertyName == nameof(DynamicTimeSeriesSegment.Entries))
            {
                var jsItems = new InternalHandle[_segment._segmentEntry.Segment.NumberOfLiveEntries];
                var i = 0;
                foreach (DynamicTimeSeriesSegment.DynamicTimeSeriesEntry entry in _segment.Entries)
                {
                    jsItems[i] = CreateDynamicTimeSeriesEntryObjectInstance(engineEx, entry);
                    i++;
                }
                return (engineEx).CreateArrayWithDisposal(jsItems);
            }

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

        private InternalHandle CreateDynamicTimeSeriesEntryObjectInstance(V8EngineEx engineEx, DynamicTimeSeriesSegment.DynamicTimeSeriesEntry entry)
        {
            var engine = (V8Engine)engineEx;

            var res = engine.CreateObject();

            res.SetProperty(nameof(entry.Tag), engine.CreateValue(entry._entry.Tag?.ToString()), V8PropertyAttributes.ReadOnly);
            res.SetProperty(nameof(entry.Timestamp), engine.CreateValue(entry._entry.Timestamp), V8PropertyAttributes.ReadOnly);
            res.SetProperty(nameof(entry.Value), engine.CreateValue(entry._entry.Values.Span[0]), V8PropertyAttributes.ReadOnly);
            
            int arrayLength =  entry._entry.Values.Length;
            var jsItems = new InternalHandle[arrayLength];
            for (int i = 0; i < arrayLength; ++i)
            {
                jsItems[i] = engine.CreateValue(entry._entry.Values.Span[i]);
            }

            res.SetProperty(nameof(entry.Values), engine.CreateArrayWithDisposal(jsItems), V8PropertyAttributes.ReadOnly);
            return res;
        }

        public class CustomBinder : CustomBinder<TimeSeriesSegmentObjectInstanceV8>
        {
        }
    }
}
