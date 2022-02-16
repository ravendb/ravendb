#nullable enable
using System;
using System.Runtime.CompilerServices;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using V8.Net;
using Raven.Client.ServerWide.JavaScript;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.Static.JavaScript.V8;
using Raven.Server.Documents.Indexes.Static.Counters.V8;
using Raven.Server.Documents.Indexes.Static.TimeSeries.V8;
using Sparrow;
using Sparrow.Json;
using Raven.Client.Util;
using Jint.Native;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Server.Config.Settings;

namespace Raven.Server.Documents.Patch.V8
{
    public class V8EngineEx : V8Engine, IJsEngineHandle
    {
        // pool of V8 engines (isolates)
        // each engine requires at least 2 Mb of memory
        // the level of a pooled value (i.e. isolate, engine) usage is the number of active contexts on it
        // isolates can not share anything (including contexts)
        // thus, a context (i.e. global object, etc.) can be set to the engine on which it was created only (not any other) 
        // an isolate can have a single active (i.e. set) context at a time, so we use locks on engine to set a context to it and run it on the context
        // too many contexts on an engine may lead to concurrent tasks waiting for its turn
        // so on a need for a new context creation we should always choose the engine with the minimal active contexts (i.e. usage level)
        // at the same time we avoid creating many contexts with low usage level, so before creating a new engine we first try to use the existing one to some configured reasonable level (targetLevel)
        // also we limit the maximum number of engines to avoid memory issues (maxCapacity)
        // PoolWithLevels<V8EngineEx>(targetLevel, maxCapacity) makes this job
        private static PoolWithLevels<V8EngineEx>? _pool;

        public static PoolWithLevels<V8EngineEx> GetPool(IJavaScriptOptions jsOptions)
        {
            if (_pool == null)
            {
                _pool = new PoolWithLevels<V8EngineEx>(jsOptions.TargetContextCountPerEngine, jsOptions.MaxEngineCount);
            }

            return _pool;
        }
    
        public class ContextEx : IDisposable
        {
            private Context? _contextNative;
            public V8Engine Engine;
            public V8EngineEx EngineEx { get { return (V8EngineEx)Engine; } }
            
            public ContextEx(V8Engine engine, ObjectTemplate? globalTemplate = null)
            {
                _contextNative = engine.CreateContext(globalTemplate);
                Engine = engine;
            }

            public void Dispose()
            {
                if (_contextNative != null)
                {
                    _contextNative.Dispose();
                    _contextNative = null;
                }
            }

            public Context ContextNative
            {
                get => _contextNative;
            }
            
            private IJavaScriptOptions? _jsOptions;

            public IJavaScriptOptions? JsOptions => _jsOptions;

        
            public void SetBasicConfiguration()
            {
                //.LocalTimeZone(TimeZoneInfo.Utc);  // TODO -> ??? maybe these V8 args: harmony_intl_locale_info, harmony_intl_more_timezone
            }

            public void SetOptions(V8Engine engine, IJavaScriptOptions? jsOptions)
            {
                _jsOptions = jsOptions;
                SetBasicConfiguration();
                if (jsOptions == null)
                    return;
                string strictModeFlag = jsOptions.StrictMode ? "--use_strict" : "--no-use_strict";
                string[] optionsCmd = {strictModeFlag}; //, "--max_old_space_size=1024"};
                engine.SetFlagsFromCommandLine(optionsCmd);
                MaxDuration = (int)jsOptions.MaxDuration.GetValue(TimeUnit.Milliseconds);
            }
            
            public int MaxDuration = 0;

            public int RefineMaxDuration(int timeout)
            {
                return timeout > 0 ? timeout : MaxDuration;
            }
        
        }
        
        
        public class JsConverter : IJsConverter
        {
            public static JsConverter Instance = new();
            
            public InternalHandle ConvertToJs(V8Engine engine, object obj, bool keepAlive = false)
            {
                return obj switch 
                {
                    LazyNumberValue lnv => engine.CreateValue(lnv.ToDouble(CultureInfo.InvariantCulture)),
                    StringSegment ss => engine.CreateValue(ss.ToString()),
                    LazyStringValue lsv => engine.CreateValue(lsv.ToString()),
                    LazyCompressedStringValue lcsv => engine.CreateValue(lcsv.ToString()),
                    Guid guid => engine.CreateValue(guid.ToString()),
                    TimeSpan timeSpan => engine.CreateValue(timeSpan.ToString()),
                    DateTime dateTime => engine.CreateValue(dateTime.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite)),
                    DateTimeOffset dateTimeOffset => engine.CreateValue(dateTimeOffset.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite)),
                    _ => InternalHandle.Empty
                };
            }
        }
        
// env object helps to distinguish between execution environments like 'RavendDB' and 'Node.js' and engines like 'V8' and 'Jint'.
// Node.js can be used both for testing and alternative execution (with modified logic).
        public const string ExecEnvCodeV8 = @"
var process = {
    env: {
        EXEC_ENV: 'RavenDB',
        ENGINE: 'V8'
    }
}
";

        public static void DisposeAndCollectGarbage(List<object> items, string? snapshotName)
        {
            V8Engine? engine = null;
            for (int i = items.Count - 1; i >= 0; i--)
            {
                var v8Handle = items[i] is InternalHandle ? (InternalHandle)items[i] : InternalHandle.Empty;
                if (!v8Handle.IsEmpty)
                {
                    if (engine == null)
                        engine = v8Handle.Engine;
                    v8Handle.Dispose();
                }
                else
                {
                    var jsHandle = items[i] is JsHandle ? (JsHandle)items[i] : JsHandle.Empty;
                    if (!jsHandle.IsEmpty)
                    {
                        if (engine == null)
                            engine = (V8Engine)jsHandle.Engine;
                        jsHandle.Dispose();
                    }
                }
            }

            engine?.ForceV8GarbageCollection();
            if (snapshotName != null)
                engine?.CheckForMemoryLeaks(snapshotName);

        }

        private ContextEx? _contextEx;
        
        public void SetContext(ContextEx ctx)
        {
            if (_contextEx == null || !ReferenceEquals(ctx, _contextEx))
            {
                _contextEx = ctx;
                SetContext(ctx.ContextNative);
            }
        }

        public static void DisposeJsObjectsIfNeeded(object value)
        {
            if (value is InternalHandle jsValue)
            {
                jsValue.Dispose();
            }
            else if (value is object[] arr)
            {
                for(int i=0; i < arr.Length; i++)
                {
                    if (arr[i] is InternalHandle jsItem)
                        jsItem.Dispose();       
                }
            }
        }

        // ------------------------------------------ IJavaScriptEngineHandle implementation
        public JavaScriptEngineType EngineType => JavaScriptEngineType.V8;

        public IJavaScriptOptions? JsOptions => _contextEx?.JsOptions;

        public void SetOptions(IJavaScriptOptions? jsOptions)
        {
            _contextEx?.SetOptions(this, jsOptions);
        }

        public IDisposable DisableConstraints()
        {
            return DisableMaxDuration();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ForceGarbageCollection()
        {
            ForceV8GarbageCollection();
        }

        public void TryCompileScript(string script)
        {
            try
            {
                using (var jsComiledScript = Compile(script, "script", true))
                {}
            }
            catch (Exception e)
            {
                throw new JavaScriptParseException("Failed to parse:" + Environment.NewLine + script, e);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Execute(string source, string sourceName = "anonymousCode.js", bool throwExceptionOnError = true)
        {
            base.Execute(source, sourceName, throwExceptionOnError);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ExecuteWithReset(string source, string sourceName = "anonymousCode.js", bool throwExceptionOnError = true)
        {
            ExecuteWithResetBase(source, sourceName, throwExceptionOnError);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ExecuteWithResetBase(string source, string sourceName = "anonymousCode.js", bool throwExceptionOnError = true, int timeout = 0)
        {
            using (ExecuteExprWithReset(source, sourceName, throwExceptionOnError, timeout))
            {}
        }

        public void ExecuteWithReset(InternalHandle script, bool throwExceptionOnError = true, int timeout = 0)
        {
            using (ExecuteExprWithReset(script, throwExceptionOnError, timeout))
            {}
        }

        public InternalHandle ExecuteExprWithReset(string source, string sourceName = "anonymousCode.js", bool throwExceptionOnError = true, int timeout = 0)
        {
            using (var script = Compile(source, sourceName, throwExceptionOnError))
            {
                return ExecuteExprWithReset(script, throwExceptionOnError, timeout);
            }
        }

        public InternalHandle ExecuteExprWithReset(InternalHandle script, bool throwExceptionOnError = true, int timeout = 0)
        {
            try
            {
                return base.Execute(script, throwExceptionOnError, _contextEx?.RefineMaxDuration(timeout) ?? 0);
            }
            finally
            {
                ResetCallStack();
                ResetConstraints();
            }
        }

        new public JsHandle GlobalObject
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new(base.GlobalObject);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandle GetGlobalProperty(string propertyName)
        {
            return new JsHandle(base.GlobalObject.GetProperty(propertyName));
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetGlobalProperty(string propertyName, JsHandle value)
        {
            base.GlobalObject.SetProperty(propertyName, value.V8.Item);
        }

        public JsHandle FromObjectGen(object obj, bool keepAlive = false)
        {
            return new JsHandle(FromObject(obj, keepAlive));
        }

        public JsHandle CreateClrCallBack(string propertyName, (Func<JsValue, JsValue[], JsValue> Jint, JSFunction V8) funcTuple, bool keepAlive = true)
        {
            return new JsHandle(CreateClrCallBack(funcTuple.V8, keepAlive));
        }

        public void SetGlobalClrCallBack(string propertyName, (Func<JsValue, JsValue[], JsValue> Jint, JSFunction V8) funcTuple)
        {
            var jsFunc = CreateClrCallBack(funcTuple.V8, true);
            if (!base.GlobalObject.SetProperty(propertyName, jsFunc))
            {
                throw new InvalidOperationException($"Failed to set global property {propertyName}");
            }            
        }

        public new JsHandle CreateObject()
        {
            return new JsHandle(base.CreateObject());
        }

        public new JsHandle CreateEmptyArray()
        {
            return new JsHandle(base.CreateEmptyArray());
        }
        
        public JsHandle CreateArray(JsHandle[] items)
        {
            int arrayLength = items.Length;
            var jsItems = new InternalHandle[arrayLength];
            for (int i = 0; i < arrayLength; ++i)
            {
                var jhItem = items[i];
                jsItems[i] = jhItem.V8.Item;
            }
            return new JsHandle(CreateArrayWithDisposal(jsItems));
        }
        
        public new JsHandle CreateArray(System.Array items)
        {
            return new JsHandle(base.CreateArray(items));
        }
        
        public new JsHandle CreateArray(IEnumerable<object> items)
        {
            return new JsHandle(base.CreateArray(items));
        }

        public JsHandle CreateUndefinedValue()
        {
            return new JsHandle(JavaScriptEngineType.V8);
        }
        
        public new JsHandle CreateNullValue()
        {
            return new JsHandle(base.CreateNullValue());
        }
        
        public new JsHandle CreateValue(bool value)
        {
            return new JsHandle(base.CreateValue(value));
        }

        public new JsHandle CreateValue(Int32 value)
        {
            return new JsHandle(base.CreateValue(value));
        }

        public new JsHandle CreateValue(double value)
        {
            return new JsHandle(base.CreateValue(value));
        }

        public new JsHandle CreateValue(string value)
        {
            return new JsHandle(base.CreateValue(value));
        }

        public new JsHandle CreateValue(TimeSpan ms)
        {
            return new JsHandle(base.CreateValue(ms));
        }

        public new JsHandle CreateValue(DateTime value)
        {
            return new JsHandle(base.CreateValue(value));
        }

        public new JsHandle CreateError(string message, JSValueType errorType)
        {
            return new JsHandle(base.CreateError(message, errorType));
        }

        // ------------------------------------------ internal implementation
        private ObjectTemplate _implicitNullTemplate;
        private ObjectTemplate _explicitNullTemplate;
        private DynamicJsNullV8? _implicitNull;
        private DynamicJsNullV8? _explicitNull;

        public InternalHandle ImplicitNullV8 => base.CreateNullValue(); // _implicitNull?.CreateHandle() ?? InternalHandle.Empty; // [shlomo] as DynamicJsNullV8 can't work as in Jint
        public InternalHandle ExplicitNullV8 => base.CreateNullValue(); // _explicitNull?.CreateHandle() ?? InternalHandle.Empty; // [shlomo] as DynamicJsNullV8 can't work as in Jint
        
        public JsHandle ImplicitNull => new(ImplicitNullV8);
        public JsHandle ExplicitNull => new(ExplicitNullV8);

        private JsHandle _jsonStringify;
        public JsHandle JsonStringify => _jsonStringify;
        
        public InternalHandle JsonStringifyV8;

        public TypeBinder? TypeBinderBlittableObjectInstance;
        public TypeBinder? TypeBinderTask;
        public TypeBinder? TypeBinderTimeSeriesSegmentObjectInstance;
        public TypeBinder? TypeBinderCounterEntryObjectInstance;
        public TypeBinder? TypeBinderAttachmentNameObjectInstance;
        public TypeBinder? TypeBinderAttachmentObjectInstance;
        public TypeBinder? TypeBinderLazyNumberValue;
        public TypeBinder? TypeBinderRavenServer;
        public TypeBinder? TypeBinderDocumentDatabase;

        public V8EngineEx() : base(false, jsConverter: JsConverter.Instance)
        {
        }

        public V8EngineEx(IJavaScriptOptions? jsOptions = null) : base(false, jsConverter: JsConverter.Instance)
        {
            if (jsOptions != null)
                CreateAndSetContextEx(jsOptions);
        }

        public ContextEx CreateAndSetContextEx(IJavaScriptOptions jsOptions, ObjectTemplate? globalTemplate = null)
        {
            var contextEx = new ContextEx(this, globalTemplate);
            SetContext(contextEx);
            SetOptions(jsOptions);
            InitializeGlobal();
            return contextEx;
        }
        
        public void InitializeGlobal()
        {
            _implicitNullTemplate = CreateObjectTemplate();
            _implicitNull = _implicitNullTemplate.CreateObject<DynamicJsNullV8>();
            _implicitNull.SetKind(false);

            _explicitNullTemplate = CreateObjectTemplate();
            _explicitNull = _explicitNullTemplate.CreateObject<DynamicJsNullV8>();
            _implicitNull.SetKind(true);

            ExecuteWithReset(ExecEnvCodeV8, "ExecEnvCode");

            TypeBinderBlittableObjectInstance = RegisterType<BlittableObjectInstanceV8>(null, true);
            TypeBinderBlittableObjectInstance.OnGetObjectBinder = (tb, obj, initializeBinder)
                => tb.CreateObjectBinder<BlittableObjectInstanceV8.CustomBinder, BlittableObjectInstanceV8>((BlittableObjectInstanceV8)obj, initializeBinder, keepAlive: true);
            base.GlobalObject.SetProperty(typeof(BlittableObjectInstanceV8));

            TypeBinderTask = RegisterType<Task>(null, true, ScriptMemberSecurity.ReadWrite);
            TypeBinderTask.OnGetObjectBinder = (tb, obj, initializeBinder)
                => tb.CreateObjectBinder<TaskCustomBinder, Task>((Task)obj, initializeBinder, keepAlive: true);
            base.GlobalObject.SetProperty(typeof(Task));
            
            TypeBinderTimeSeriesSegmentObjectInstance = RegisterType<TimeSeriesSegmentObjectInstanceV8>(null, false);
            TypeBinderTimeSeriesSegmentObjectInstance.OnGetObjectBinder = (tb, obj, initializeBinder)
                => tb.CreateObjectBinder<TimeSeriesSegmentObjectInstanceV8.CustomBinder, TimeSeriesSegmentObjectInstanceV8>((TimeSeriesSegmentObjectInstanceV8)obj, initializeBinder, keepAlive: true);
            base.GlobalObject.SetProperty(typeof(TimeSeriesSegmentObjectInstanceV8));

            TypeBinderCounterEntryObjectInstance = RegisterType<CounterEntryObjectInstanceV8>(null, false);
            TypeBinderCounterEntryObjectInstance.OnGetObjectBinder = (tb, obj, initializeBinder)
                => tb.CreateObjectBinder<CounterEntryObjectInstanceV8.CustomBinder, CounterEntryObjectInstanceV8>((CounterEntryObjectInstanceV8)obj, initializeBinder, keepAlive: true);
            base.GlobalObject.SetProperty(typeof(CounterEntryObjectInstanceV8));

            TypeBinderAttachmentNameObjectInstance = RegisterType<AttachmentNameObjectInstanceV8>(null, false);
            TypeBinderAttachmentNameObjectInstance.OnGetObjectBinder = (tb, obj, initializeBinder)
                => tb.CreateObjectBinder<AttachmentNameObjectInstanceV8.CustomBinder, AttachmentNameObjectInstanceV8>((AttachmentNameObjectInstanceV8)obj, initializeBinder, keepAlive: true);
            base.GlobalObject.SetProperty(typeof(AttachmentNameObjectInstanceV8));

            TypeBinderAttachmentObjectInstance = RegisterType<AttachmentObjectInstanceV8>(null, false);
            TypeBinderAttachmentObjectInstance.OnGetObjectBinder = (tb, obj, initializeBinder)
                => tb.CreateObjectBinder<AttachmentObjectInstanceV8.CustomBinder, AttachmentObjectInstanceV8>((AttachmentObjectInstanceV8)obj, initializeBinder, keepAlive: true);
            base.GlobalObject.SetProperty(typeof(AttachmentObjectInstanceV8));

            TypeBinderLazyNumberValue = RegisterType<LazyNumberValue>(null, false);
            TypeBinderLazyNumberValue.OnGetObjectBinder = (tb, obj, initializeBinder)
                => tb.CreateObjectBinder<ObjectBinder, LazyNumberValue>((LazyNumberValue)obj, initializeBinder, keepAlive: true);
            base.GlobalObject.SetProperty(typeof(LazyNumberValue));

            TypeBinderRavenServer = RegisterType<RavenServer>(null, true, ScriptMemberSecurity.ReadWrite);
            TypeBinderRavenServer.OnGetObjectBinder = (tb, obj, initializeBinder)
                => tb.CreateObjectBinder<ObjectBinder, RavenServer>((RavenServer)obj, initializeBinder, keepAlive: true);
            base.GlobalObject.SetProperty(typeof(RavenServer));
                
            TypeBinderDocumentDatabase = RegisterType<DocumentDatabase>(null, true, ScriptMemberSecurity.ReadWrite);
            TypeBinderDocumentDatabase.OnGetObjectBinder = (tb, obj, initializeBinder)
                => tb.CreateObjectBinder<ObjectBinder, DocumentDatabase>((DocumentDatabase)obj, initializeBinder, keepAlive: true);
            base.GlobalObject.SetProperty(typeof(DocumentDatabase));

            JsonStringifyV8 = this.Execute("JSON.stringify", "JSON.stringify", true, 0);
            _jsonStringify = new JsHandle(JsonStringifyV8);
        }

        public override void Dispose() 
        {
            JsonStringifyV8.Dispose();
            base.Dispose();
        }

        public IDisposable ChangeMaxStatements(int maxDurationNew)
        {
            // doing nothing as V8 doesn't support limiting MaxStatements
            
            void RestoreMaxStatements()
            {
            }
            return new DisposableAction(RestoreMaxStatements);
            
        }
        
        public IDisposable ChangeMaxDuration(int maxDurationNew)
        {
            if (_contextEx == null)
                return new DisposableAction(() => { });

            int maxDurationPrev = _contextEx.MaxDuration;

            void RestoreMaxDuration()
            {
                _contextEx.MaxDuration = maxDurationPrev;
            }

            _contextEx.MaxDuration = maxDurationNew;
            return new DisposableAction(RestoreMaxDuration);
        }

        public IDisposable DisableMaxDuration()
        {
            return ChangeMaxDuration(0);
        }
        
        public void ResetCallStack()
        {
            //engine?.ForceV8GarbageCollection();

            // there is no need to do something as V8 doesn't have intermediate state of callstack
        }

        public void ResetConstraints()
        {
            // there is no need to do something as V8 doesn't have intermediate state of timer
        }

    }
}
