using System;
using Raven.Server.Documents.Indexes.Static.Counters;
using Raven.Server.Documents.Indexes.Static.TimeSeries;
using Sparrow.Json;

namespace Raven.Server.Documents.Patch
{
    public interface IJavaScriptUtils<T> : IJavaScriptUtilsClearance
        where T : struct, IJsHandle<T>
    {
        IJsEngineHandle<T> EngineHandle { get; }

        JsonOperationContext Context { get; }
        bool ReadOnly { get; set; }

        IBlittableObjectInstance CreateBlittableObjectInstanceFromScratch(IBlittableObjectInstance parent,
            BlittableJsonReaderObject blittable,
            string id,
            DateTime? lastModified,
            string changeVector);

        IBlittableObjectInstance CreateBlittableObjectInstanceFromDoc(IBlittableObjectInstance parent,
            BlittableJsonReaderObject blittable,
            Document doc);

        IObjectInstance<T> CreateTimeSeriesSegmentObjectInstance(DynamicTimeSeriesSegment segment);

        IObjectInstance<T> CreateCounterEntryObjectInstance(DynamicCounterEntry entry);


        T GetDocumentId(T self, T[] args);
        T AttachmentsFor(T self, T[] args);
        T GetMetadata(T self, T[] args);
        T GetTimeSeriesNamesFor(T self, T[] args);
        T GetCounterNamesFor(T self, T[] args);
        T LoadAttachment(T self, T[] args);
        T LoadAttachments(T self, T[] args);
        T TranslateToJs(JsonOperationContext context, object o, bool keepAlive = false);
    }

    public interface IJavaScriptUtilsClearance
    {

        void Clear();

        void Reset(JsonOperationContext ctx);

    }
}
