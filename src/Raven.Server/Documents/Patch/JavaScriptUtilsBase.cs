using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Raven.Client.ServerWide.JavaScript;
using Sparrow.Json;
using Raven.Server.Documents.Indexes.Static.TimeSeries;
using Raven.Server.Documents.Indexes.Static.TimeSeries.Jint;
using Raven.Server.Documents.Indexes.Static.TimeSeries.V8;
using Raven.Server.Documents.Indexes.Static.Counters;
using Raven.Server.Documents.Indexes.Static.Counters.Jint;
using Raven.Server.Documents.Indexes.Static.Counters.V8;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Server.Documents.Queries.Results.TimeSeries;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Documents.Patch.V8;

namespace Raven.Server.Documents.Patch
{
    public abstract class JavaScriptUtilsBase : IJavaScriptUtils
    {
        private IJsEngineHandle _engine;
        
        public IJsEngineHandle EngineHandle => _engine;

        public JsonOperationContext Context
        {
            get
            {
                Debug.Assert(_context != null, "_context != null");
                return _context;
            }
        }

        protected JsonOperationContext _context;
        protected readonly ScriptRunner _runnerBase;
        protected readonly List<IDisposable> _disposables = new List<IDisposable>();

        private bool _readOnly;

        public bool ReadOnly
        {
            get { return _readOnly; }
            set { _readOnly = value; }
        }

        private JavaScriptEngineType _jsEngineType;

        public JavaScriptUtilsBase(ScriptRunner runner, IJsEngineHandle engine)
        {
            _runnerBase = runner;
            _jsEngineType = engine.EngineType;
            _engine = engine;
        }

        public static IJavaScriptUtils Create(ScriptRunner runner, IJsEngineHandle engineHandle)
        {
            var jsEngineType = engineHandle.EngineType;
            return jsEngineType switch
            {
                JavaScriptEngineType.Jint => new JavaScriptUtilsJint(runner, (JintEngineEx)engineHandle),
                JavaScriptEngineType.V8 => new JavaScriptUtilsV8(runner, (V8EngineEx)engineHandle),
                _ => throw new NotSupportedException($"Not supported JS engine kind '{jsEngineType}'.")
            };
        }

        public IBlittableObjectInstance CreateBlittableObjectInstanceFromScratch(IJavaScriptUtils javaScriptUtils,
            IBlittableObjectInstance parent,
            BlittableJsonReaderObject blittable,
            string id,
            DateTime? lastModified,
            string changeVector)
        {
            return _jsEngineType switch
            {
                JavaScriptEngineType.Jint => new BlittableObjectInstanceJint(((JavaScriptUtilsJint)javaScriptUtils).Engine, (BlittableObjectInstanceJint)parent, blittable, id, lastModified, changeVector),
                JavaScriptEngineType.V8 => new BlittableObjectInstanceV8((JavaScriptUtilsV8)javaScriptUtils, (BlittableObjectInstanceV8)parent, blittable, id, lastModified, changeVector),
                _ => throw new NotSupportedException($"Not supported JS engine kind '{_jsEngineType}'.")
            };
        }

        public IBlittableObjectInstance CreateBlittableObjectInstanceFromDoc(IJavaScriptUtils javaScriptUtils,
            IBlittableObjectInstance parent,
            BlittableJsonReaderObject blittable,
            Document doc)
        {
            return _jsEngineType switch
            {
                JavaScriptEngineType.Jint => new BlittableObjectInstanceJint(((JavaScriptUtilsJint)javaScriptUtils).Engine, (BlittableObjectInstanceJint)parent, blittable, doc),
                JavaScriptEngineType.V8 => new BlittableObjectInstanceV8((JavaScriptUtilsV8)javaScriptUtils, (BlittableObjectInstanceV8)parent, blittable, doc),
                _ => throw new NotSupportedException($"Not supported JS engine kind '{_jsEngineType}'.")
            };
        }

        public IObjectInstance CreateTimeSeriesSegmentObjectInstance(IJsEngineHandle engineHandle, DynamicTimeSeriesSegment segment)
        {
            return _jsEngineType switch
            {
                JavaScriptEngineType.Jint => new TimeSeriesSegmentObjectInstanceJint((JintEngineEx)engineHandle, segment),
                JavaScriptEngineType.V8 => new TimeSeriesSegmentObjectInstanceV8((V8EngineEx)engineHandle, segment),
                _ => throw new NotSupportedException($"Not supported JS engine kind '{_jsEngineType}'.")
            };
        }

        public IObjectInstance CreateCounterEntryObjectInstance(IJsEngineHandle engineHandle, DynamicCounterEntry entry)
        {
            return _jsEngineType switch
            {
                JavaScriptEngineType.Jint => new CounterEntryObjectInstanceJint((JintEngineEx)engineHandle, entry),
                JavaScriptEngineType.V8 => new CounterEntryObjectInstanceV8((V8EngineEx)engineHandle, entry),
                _ => throw new NotSupportedException($"Not supported JS engine kind '{_jsEngineType}'.")
            };
        }
        
        protected void AssertAdminScriptInstance()
        {
            if (_runnerBase._enableClr == false)
                throw new InvalidOperationException("Unable to run admin scripts using this instance of the script runner, the EnableClr is set to false");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected BlittableJsonReaderObject Clone(BlittableJsonReaderObject origin, JsonOperationContext context)
        {
            if (ReadOnly)
                return origin;

            var noCache = origin.NoCache;
            origin.NoCache = true;
            // RavenDB-8286
            // here we need to make sure that we aren't sending a value to
            // the js engine that might be modified by the actions of the js engine
            // for example, calling put() might cause the original data to change
            // because we defrag the data that we looked at. We are handling this by
            // ensuring that we have our own, safe, copy.
            var cloned = origin.Clone(context);
            cloned.NoCache = true;
            _disposables.Add(cloned);

            origin.NoCache = noCache;
            return cloned;
        }

        public void Clear()
        {
            foreach (var disposable in _disposables)
                disposable.Dispose();
            _disposables.Clear();
            _context = null;
        }

        public void Reset(JsonOperationContext ctx)
        {
            _context = ctx;
        }

        public JsHandle TranslateToJs(JsonOperationContext context, object o, bool keepAlive = false)
        {
            if (o is JsHandle jsValue)
                return jsValue.Clone();

            if (this is JavaScriptUtilsJint jsUtilsJint)
                return new JsHandle(jsUtilsJint.TranslateToJs(context, o));
            if (this is JavaScriptUtilsV8 jsUtilsV8)
                return new JsHandle(jsUtilsV8.TranslateToJs(context, o, keepAlive));
            throw new InvalidOperationException($"Not supported JsHandleType '{_jsEngineType}'.");
        }
    }
}
