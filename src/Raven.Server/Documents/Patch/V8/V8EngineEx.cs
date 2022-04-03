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
        // so we try to keep the minimum number on engines having the number of contexts close to the target level
        // thus, on a need for a new context creation we should always choose the engine with the actual number of contexts that is the closest one to the target one (below target are more preferrable)  
        // at the same time we avoid creating many contexts with low usage level, so before creating a new engine we first try to use the existing one to some configured reasonable level (targetLevel)
        // also we limit the maximum number of engines to avoid memory issues (maxCapacity)
        // PoolWithLevels<V8EngineEx>(targetLevel, maxCapacity) makes this job
        private static PoolWithLevels<V8EngineEx>? _pool;

        public static int MemoryChecksMode;
        public static bool IsMemoryChecksOnStatic => false; // MemoryChecksMode > 0; // TODO [shlomo] to restore
        public static JsConverter JsConverterInstance;

        public static PoolWithLevels<V8EngineEx> GetPool(IJavaScriptOptions jsOptions)
        {
            if (_pool == null)
            {
                _pool = new PoolWithLevels<V8EngineEx>(jsOptions.TargetContextCountPerEngine, jsOptions.MaxEngineCount);
                MemoryChecksMode = int.Parse(Environment.GetEnvironmentVariable("JS_V8_MemoryChecksMode") ?? "0");
                JsConverterInstance = new(IsMemoryChecksOnStatic);
            }

            return _pool;
        }
    
        public class ContextEx : IDisposable
        {
            private Context? _contextNative;
            public V8Engine Engine;
            public V8EngineEx EngineEx { get { return (V8EngineEx)Engine; } }

            public ContextEx(V8Engine engine, IJavaScriptContext jsContext, ObjectTemplate? globalTemplate = null)
            {
                _contextNative = engine.CreateContext(globalTemplate);
                Engine = engine;
                _jsContext = jsContext;
            }

            public void Dispose()
            {
                JsonStringifyV8().Dispose();
                
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

        
            private IJavaScriptContext _jsContext;
            public IJavaScriptContext JsContext => _jsContext;
            
            public void SetOptions(IJavaScriptOptions? jsOptions)
            {
                _jsOptions = jsOptions;
                if (jsOptions == null)
                    return;
                string strictModeFlag = jsOptions.StrictMode ? "--use_strict" : "--no-use_strict";
                string[] optionsCmd = {strictModeFlag};
                Engine.SetFlagsFromCommandLine(optionsCmd);
                _contextNative.MaxDuration = (int)jsOptions.MaxDuration.GetValue(TimeUnit.Milliseconds);
            }
            
            // -----------------------global object related-----------------------------
            private InternalHandle _implicitNullV8 = InternalHandle.Empty;
            private InternalHandle _explicitNullV8 = InternalHandle.Empty;

            public InternalHandle ImplicitNullV8()
            {
                if (_implicitNullV8.IsEmpty)
                {
                    _implicitNullV8 = Engine.CreateNullValue(); // _implicitNull?.CreateHandle() ?? InternalHandle.Empty; // [shlomo] as DynamicJsNullV8 can't work as in Jint
                    Engine.AddToLastMemorySnapshotBefore(_implicitNullV8);
                }
                return _implicitNullV8;
            }
            
            public InternalHandle ExplicitNullV8()
            {
                if (_explicitNullV8.IsEmpty)
                {
                    _explicitNullV8 = Engine.CreateNullValue(); // _explicitNull?.CreateHandle() ?? InternalHandle.Empty; // [shlomo] as DynamicJsNullV8 can't work as in Jint
                    Engine.AddToLastMemorySnapshotBefore(_explicitNullV8);
                }
                return _explicitNullV8;
            }

            /*private DynamicJsNullV8? _implicitNull;
            private DynamicJsNullV8? _explicitNull;

            _implicitNull = Engine.CreateObjectTemplate().CreateObject<DynamicJsNullV8>();
            _implicitNull.SetKind(false);

            _explicitNull = Engine.CreateObjectTemplate().CreateObject<DynamicJsNullV8>();
            _explicitNull.SetKind(true);*/


            
            private InternalHandle _jsonStringifyV8 = InternalHandle.Empty;

            public InternalHandle JsonStringifyV8()
            {
                if (_jsonStringifyV8.IsEmpty)
                {
                    _jsonStringifyV8 = Engine.Execute("JSON.stringify", "JSON.stringify", true, 0);
                    Engine.AddToLastMemorySnapshotBefore(_jsonStringifyV8);
                }
                return _jsonStringifyV8;
            }

            private JsHandle _jsonStringify = JsHandle.Empty;
            private JsHandle _implicitNull = JsHandle.Empty;
            private JsHandle _explicitNull = JsHandle.Empty;

            public JsHandle JsonStringify()
            {
                if (_jsonStringify.IsEmpty)
                {
                    _jsonStringify = new JsHandle(JsonStringifyV8());
                }
                return _jsonStringify;
            }

            public JsHandle ImplicitNull()
            {
                if (_implicitNull.IsEmpty)
                {
                    _implicitNull = new JsHandle(ImplicitNullV8());
                }
                return _implicitNull;
            }

            public JsHandle ExplicitNull()
            {
                if (_explicitNull.IsEmpty)
                {
                    _explicitNull = new JsHandle(ExplicitNullV8());
                }
                return _explicitNull;
            }


            private TypeBinder? _typeBinderTask;
            private TypeBinder? _typeBinderBlittableObjectInstance;
            private TypeBinder? _typeBinderTimeSeriesSegmentObjectInstance;
            private TypeBinder? _typeBinderCounterEntryObjectInstance;
            private TypeBinder? _typeBinderAttachmentNameObjectInstance;
            private TypeBinder? _typeBinderAttachmentObjectInstance;
            private TypeBinder? _typeBinderLazyNumberValue;
            private TypeBinder? _typeBinderLazyStringValue;
            private TypeBinder? _typeBinderLazyCompressedStringValue;
            private TypeBinder? _typeBinderRavenServer;
            private TypeBinder? _typeBinderDocumentDatabase;

            public TypeBinder? TypeBinderBlittableObjectInstance()
            {
                if (_typeBinderBlittableObjectInstance == null)
                {
                    _typeBinderBlittableObjectInstance = Engine.RegisterType<BlittableObjectInstanceV8>(null, false, useLazy: false);
                    _typeBinderBlittableObjectInstance.OnGetObjectBinder = (tb, obj, initializeBinder)
                        => tb.CreateObjectBinder<BlittableObjectInstanceV8.CustomBinder, BlittableObjectInstanceV8>((BlittableObjectInstanceV8)obj, initializeBinder,
                            keepAlive: true);
                    Engine.GlobalObject.SetProperty(typeof(BlittableObjectInstanceV8), addToLastMemorySnapshotBefore: true);
                }
                return _typeBinderBlittableObjectInstance;
            }

            public TypeBinder? TypeBinderTask()
            {
                if (_typeBinderTask == null)
                {
                    _typeBinderTask = Engine.RegisterType<Task>(null, false, ScriptMemberSecurity.ReadWrite, useLazy: false);
                    _typeBinderTask.OnGetObjectBinder = (tb, obj, initializeBinder)
                        => tb.CreateObjectBinder<TaskCustomBinder, Task>((Task)obj, initializeBinder, keepAlive: true);
                    Engine.GlobalObject.SetProperty(typeof(Task), addToLastMemorySnapshotBefore: true);
                }
                return _typeBinderTask;
            }

            public TypeBinder? TypeBinderTimeSeriesSegmentObjectInstance()
            {
                if (_typeBinderTimeSeriesSegmentObjectInstance == null)
                {
                    _typeBinderTimeSeriesSegmentObjectInstance = Engine.RegisterType<TimeSeriesSegmentObjectInstanceV8>(null, false, useLazy: false);
                    _typeBinderTimeSeriesSegmentObjectInstance.OnGetObjectBinder = (tb, obj, initializeBinder)
                        => tb.CreateObjectBinder<TimeSeriesSegmentObjectInstanceV8.CustomBinder, TimeSeriesSegmentObjectInstanceV8>((TimeSeriesSegmentObjectInstanceV8)obj, initializeBinder, keepAlive: true);
                    Engine.GlobalObject.SetProperty(typeof(TimeSeriesSegmentObjectInstanceV8), addToLastMemorySnapshotBefore: true);
                }
                return _typeBinderTimeSeriesSegmentObjectInstance;
            }
            
            public TypeBinder? TypeBinderCounterEntryObjectInstance()
            {
                if (_typeBinderCounterEntryObjectInstance == null)
                {
                    _typeBinderCounterEntryObjectInstance = Engine.RegisterType<CounterEntryObjectInstanceV8>(null, false, useLazy: false);
                    _typeBinderCounterEntryObjectInstance.OnGetObjectBinder = (tb, obj, initializeBinder)
                        => tb.CreateObjectBinder<CounterEntryObjectInstanceV8.CustomBinder, CounterEntryObjectInstanceV8>((CounterEntryObjectInstanceV8)obj, initializeBinder, keepAlive: true);
                    Engine.GlobalObject.SetProperty(typeof(CounterEntryObjectInstanceV8), addToLastMemorySnapshotBefore: true);
                }
                return _typeBinderCounterEntryObjectInstance;
            }
            
            public TypeBinder? TypeBinderAttachmentNameObjectInstance()
            {
                if (_typeBinderAttachmentNameObjectInstance == null)
                {
                    _typeBinderAttachmentNameObjectInstance = Engine.RegisterType<AttachmentNameObjectInstanceV8>(null, false, useLazy: false);
                    _typeBinderAttachmentNameObjectInstance.OnGetObjectBinder = (tb, obj, initializeBinder)
                        => tb.CreateObjectBinder<AttachmentNameObjectInstanceV8.CustomBinder, AttachmentNameObjectInstanceV8>((AttachmentNameObjectInstanceV8)obj, initializeBinder, keepAlive: true);
                    Engine.GlobalObject.SetProperty(typeof(AttachmentNameObjectInstanceV8), addToLastMemorySnapshotBefore: true);
                }
                return _typeBinderAttachmentNameObjectInstance;
            }
            
            public TypeBinder? TypeBinderAttachmentObjectInstance()
            {
                if (_typeBinderAttachmentObjectInstance == null)
                {
                    _typeBinderAttachmentObjectInstance = Engine.RegisterType<AttachmentObjectInstanceV8>(null, false, useLazy: false);
                    _typeBinderAttachmentObjectInstance.OnGetObjectBinder = (tb, obj, initializeBinder)
                        => tb.CreateObjectBinder<AttachmentObjectInstanceV8.CustomBinder, AttachmentObjectInstanceV8>((AttachmentObjectInstanceV8)obj, initializeBinder, keepAlive: true);
                    Engine.GlobalObject.SetProperty(typeof(AttachmentObjectInstanceV8), addToLastMemorySnapshotBefore: true);
                }
                return _typeBinderAttachmentObjectInstance;
            }
            
            public TypeBinder? TypeBinderLazyNumberValue()
            {
                if (_typeBinderLazyNumberValue == null)
                {
                    _typeBinderLazyNumberValue = Engine.RegisterType<LazyNumberValue>(null, false, useLazy: false);
                    _typeBinderLazyNumberValue.OnGetObjectBinder = (tb, obj, initializeBinder)
                        => tb.CreateObjectBinder<ObjectBinder, LazyNumberValue>((LazyNumberValue)obj, initializeBinder, keepAlive: true);
                    Engine.GlobalObject.SetProperty(typeof(LazyNumberValue), addToLastMemorySnapshotBefore: true);
                }
                return _typeBinderLazyNumberValue;
            }
            
            public TypeBinder? TypeBinderLazyStringValue()
            {
                if (_typeBinderLazyStringValue == null)
                {
                    _typeBinderLazyStringValue = Engine.RegisterType<LazyStringValue>(null, false, useLazy: false);
                    _typeBinderLazyStringValue.OnGetObjectBinder = (tb, obj, initializeBinder)
                        => tb.CreateObjectBinder<ObjectBinder, LazyStringValue>((LazyStringValue)obj, initializeBinder, keepAlive: true);
                    Engine.GlobalObject.SetProperty(typeof(LazyStringValue), addToLastMemorySnapshotBefore: true);
                }
                return _typeBinderLazyStringValue;
            }
            
            public TypeBinder? TypeBinderLazyCompressedStringValue()
            {
                if (_typeBinderLazyCompressedStringValue == null)
                {
                    _typeBinderLazyCompressedStringValue = Engine.RegisterType<LazyCompressedStringValue>(null, false, useLazy: false);
                    _typeBinderLazyCompressedStringValue.OnGetObjectBinder = (tb, obj, initializeBinder)
                        => tb.CreateObjectBinder<ObjectBinder, LazyCompressedStringValue>((LazyCompressedStringValue)obj, initializeBinder, keepAlive: true);
                    Engine.GlobalObject.SetProperty(typeof(LazyCompressedStringValue), addToLastMemorySnapshotBefore: true);
                }
                return _typeBinderLazyCompressedStringValue;
            }
            
            public TypeBinder? TypeBinderRavenServer()
            {
                if (_typeBinderRavenServer == null)
                {
                    _typeBinderRavenServer = Engine.RegisterType<RavenServer>(null, true, ScriptMemberSecurity.ReadWrite, useLazy: false);
                    _typeBinderRavenServer.OnGetObjectBinder = (tb, obj, initializeBinder)
                        => tb.CreateObjectBinder<ObjectBinder, RavenServer>((RavenServer)obj, initializeBinder, keepAlive: true);
                    Engine.GlobalObject.SetProperty(typeof(RavenServer), addToLastMemorySnapshotBefore: true);
                }
                return _typeBinderRavenServer;
            }
            
            public TypeBinder? TypeBinderDocumentDatabase()
            {
                if (_typeBinderDocumentDatabase == null)
                {
                    _typeBinderDocumentDatabase = Engine.RegisterType<DocumentDatabase>(null, true, ScriptMemberSecurity.ReadWrite, useLazy: false);
                    _typeBinderDocumentDatabase.OnGetObjectBinder = (tb, obj, initializeBinder)
                        => tb.CreateObjectBinder<ObjectBinder, DocumentDatabase>((DocumentDatabase)obj, initializeBinder, keepAlive: true);
                    Engine.GlobalObject.SetProperty(typeof(DocumentDatabase), addToLastMemorySnapshotBefore: true);
                }
                return _typeBinderDocumentDatabase;
            }
            
            public void InitializeGlobal()
            {
                var bindersLazy = Engine.BindersLazy;
                
                AddToBindersLazy(bindersLazy, typeof(Task), () => TypeBinderTask());
                AddToBindersLazy(bindersLazy, typeof(BlittableObjectInstanceV8), () => TypeBinderBlittableObjectInstance());
                AddToBindersLazy(bindersLazy, typeof(TimeSeriesSegmentObjectInstanceV8), () => TypeBinderTimeSeriesSegmentObjectInstance());
                AddToBindersLazy(bindersLazy, typeof(CounterEntryObjectInstanceV8), () => TypeBinderCounterEntryObjectInstance());
                AddToBindersLazy(bindersLazy, typeof(AttachmentNameObjectInstanceV8), () => TypeBinderAttachmentNameObjectInstance());
                AddToBindersLazy(bindersLazy, typeof(AttachmentObjectInstanceV8), () => TypeBinderAttachmentObjectInstance());
                AddToBindersLazy(bindersLazy, typeof(LazyNumberValue), () => TypeBinderLazyNumberValue());
                AddToBindersLazy(bindersLazy, typeof(LazyStringValue), () => TypeBinderLazyStringValue());
                AddToBindersLazy(bindersLazy, typeof(LazyCompressedStringValue), () => TypeBinderLazyCompressedStringValue());
                AddToBindersLazy(bindersLazy, typeof(RavenServer), () => TypeBinderRavenServer());
                AddToBindersLazy(bindersLazy, typeof(DocumentDatabase), () => TypeBinderDocumentDatabase());
                
                /*var tb = TypeBinderTask();
                tb = TypeBinderBlittableObjectInstance();
                tb = TypeBinderTimeSeriesSegmentObjectInstance();
                tb = TypeBinderCounterEntryObjectInstance();
                tb = TypeBinderAttachmentNameObjectInstance();
                tb = TypeBinderAttachmentObjectInstance();
                tb = TypeBinderLazyNumberValue();
                tb = TypeBinderLazyStringValue();
                tb = TypeBinderLazyCompressedStringValue();
                tb = TypeBinderRavenServer();
                tb = TypeBinderDocumentDatabase();
                
                var h = ImplicitNull();
                h = ExplicitNull();
                h = JsonStringify();*/

                Engine.ExecuteWithReset(ExecEnvCodeV8, "ExecEnvCode");
            }
            
            private void AddToBindersLazy(Dictionary<Type, Func<TypeBinder>> bindersLazy, Type key, Func<TypeBinder> value)
            {
                bindersLazy.Add(key, value);
            }
        
        }

        public class JsConverter : IJsConverter
        {
            private bool _isMemoryChecksOn;
                
            public JsConverter(bool isMemoryChecksOn)
            {
                _isMemoryChecksOn = isMemoryChecksOn;
            }
            
            public bool IsMemoryChecksOn => _isMemoryChecksOn;
            
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

        public static void DisposeAndCollectGarbage(List<object> items, string snapshotName)
        {
            V8Engine? engineV8 = null;
            for (int i = items.Count - 1; i >= 0; i--)
            {
                var v8Handle = items[i] is InternalHandle ? (InternalHandle)items[i] : InternalHandle.Empty;
                if (!v8Handle.IsEmpty)
                {
                    if (engineV8 == null)
                        engineV8 = v8Handle.Engine;

                    if (engineV8 != null && engineV8.IsMemoryChecksOn)
                        engineV8.RemoveFromLastMemorySnapshotBefore(v8Handle);

                    v8Handle.Dispose();
                }
                else
                {
                    var jsHandle = items[i] is JsHandle ? (JsHandle)items[i] : JsHandle.Empty;
                    if (!jsHandle.IsEmpty)
                    {
                        if (engineV8 == null)
                            engineV8 = (V8Engine)jsHandle.Engine;

                        if (engineV8 != null && engineV8.IsMemoryChecksOn)
                            engineV8.RemoveFromLastMemorySnapshotBefore(jsHandle.V8.Item);

                        jsHandle.Dispose();
                    }
                }
            }

            if (engineV8 != null && engineV8.IsMemoryChecksOn)
            {
                engineV8.ForceV8GarbageCollection();
                engineV8.CheckForMemoryLeaks(snapshotName, shouldRemove: false);
            }
        }

        private ContextEx? _contextEx;
        
        public ContextEx Context
        {
            get => _contextEx;
            set 
            {
                if (_contextEx == null || !ReferenceEquals(value, _contextEx))
                {
                    _contextEx = value;
                    SetContext(value.ContextNative);
                }
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

        public IJavaScriptOptions? JsOptions => Context.JsOptions;

        public JsHandle ImplicitNull() => Context.ImplicitNull();
        public JsHandle ExplicitNull() => Context.ExplicitNull();

        public JsHandle JsonStringify() => Context.JsonStringify();

        public IDisposable DisableConstraints()
        {
            return DisableMaxDuration();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ForceGarbageCollection()
        {
            ForceV8GarbageCollection();
        }

        new public object MakeSnapshot(string name)
        {
            return base.MakeMemorySnapshot(name);
        }

        public bool RemoveMemorySnapshot(string name)
        {
            return base.RemoveMemorySnapshot(name);
        }

        public void AddToLastMemorySnapshotBefore(JsHandle h)
        {
            AddToLastMemorySnapshotBefore(h.V8.Item);
        }

        public void RemoveFromLastMemorySnapshotBefore(JsHandle h)
        {
            RemoveFromLastMemorySnapshotBefore(h.V8.Item);
        }
        
        public void TryCompileScript(string script)
        {
            try
            {
                using (Compile(script, "script", true))
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

        new public void ExecuteWithReset(InternalHandle script, bool throwExceptionOnError = true, int timeout = 0)
        {
            using (ExecuteExprWithReset(script, throwExceptionOnError, timeout))
            {}
        }

        new public InternalHandle ExecuteExprWithReset(string source, string sourceName = "anonymousCode.js", bool throwExceptionOnError = true, int timeout = 0)
        {
            using (var script = Compile(source, sourceName, throwExceptionOnError))
            {
                return ExecuteExprWithReset(script, throwExceptionOnError, timeout);
            }
        }

        new public InternalHandle ExecuteExprWithReset(InternalHandle script, bool throwExceptionOnError = true, int timeout = 0)
        {
            try
            {
                return base.Execute(script, throwExceptionOnError, RefineMaxDuration(timeout));
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
        public V8EngineEx() : base(false, jsConverter: JsConverterInstance)
        {
        }

        public ContextEx CreateAndSetContextEx(IJavaScriptOptions jsOptions, IJavaScriptContext jsContext, ObjectTemplate? globalTemplate = null)
        {
            var contextEx = new ContextEx(this, jsContext, globalTemplate);
            Context = contextEx;
            contextEx.SetOptions(jsOptions);
            contextEx.InitializeGlobal();
            return contextEx;
        }
        
        public override void Dispose() 
        {
            Context?.Dispose();
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
    }
}
