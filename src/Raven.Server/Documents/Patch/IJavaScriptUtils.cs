using System;
using System.Runtime.InteropServices;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime;
using Jint.Runtime.Interop;
using V8.Net;
using Raven.Server.Extensions.V8;
using Raven.Server.Extensions.Jint;
using Sparrow.Json;
using Sparrow.Server.Utils;
using Raven.Server.Documents.Indexes.Static.TimeSeries;
using Raven.Server.Documents.Indexes.Static.Counters;

namespace Raven.Server.Documents.Patch
{
    public interface IJavaScriptUtils
    {
        IJsEngineHandle EngineHandle { get; }

        JsonOperationContext Context { get; }
        bool ReadOnly { get; set; }

        IBlittableObjectInstance CreateBlittableObjectInstanceFromScratch(IJavaScriptUtils javaScriptUtils,
            IBlittableObjectInstance parent,
            BlittableJsonReaderObject blittable,
            string id,
            DateTime? lastModified,
            string changeVector);

        IBlittableObjectInstance CreateBlittableObjectInstanceFromDoc(IJavaScriptUtils javaScriptUtils,
            IBlittableObjectInstance parent,
            BlittableJsonReaderObject blittable,
            Document doc);

        IObjectInstance CreateTimeSeriesSegmentObjectInstance(IJsEngineHandle engineHandle, DynamicTimeSeriesSegment segment);

        IObjectInstance CreateCounterEntryObjectInstance(IJsEngineHandle engineHandle, DynamicCounterEntry entry);

        void Clear();

        void Reset(JsonOperationContext ctx);
    }
}
