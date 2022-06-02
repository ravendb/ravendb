using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Client.ServerWide.JavaScript;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Indexes.Static.Counters.V8;
using Raven.Server.Documents.Indexes.Static.JavaScript.V8;
using Raven.Server.Documents.Indexes.Static.TimeSeries.V8;
using Sparrow;
using Sparrow.Json;
using V8.Net;

namespace Raven.Server.Documents.Patch.V8;

public class V8EngineEx : IJsEngineHandle<JsHandleV8>, IDisposable
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

    public static PoolWithLevels<V8EngineEx> GetPool(RavenConfiguration configuration)
    {
        var jsOptions = configuration.JavaScript;
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
        // public V8EngineEx EngineEx { get { return (V8EngineEx)Engine; } }

        public ContextEx(V8Engine engine, IJavaScriptContext jsContext, ObjectTemplate? globalTemplate = null)
        {
            _contextNative = engine.CreateContext(globalTemplate);
            Engine = engine;
            _jsContext = jsContext;
        }

        public void Dispose()
        {
            JsonStringify().Dispose();

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

        public void SetOptions(RavenConfiguration configuration)
        {
            var jsOptions = configuration.JavaScript;
            _jsOptions = jsOptions;
            if (jsOptions == null)
                return;
            string strictModeFlag = jsOptions.StrictMode ? "--use_strict" : "--no-use_strict";
            string[] optionsCmd = { strictModeFlag };
            Engine.SetFlagsFromCommandLine(optionsCmd);
            _contextNative.MaxDuration = (int)jsOptions.MaxDuration.GetValue(TimeUnit.Milliseconds);
        }

        // TODO: egor why we need those?
        private JsHandleV8 _jsonStringify = JsHandleV8.Empty;
        private InternalHandle _jsonStringifyV8 = InternalHandle.Empty;

        public JsHandleV8 JsonStringify()
        {
            if (_jsonStringify.IsEmpty)
            {
                _jsonStringifyV8 = Engine.Execute("JSON.stringify", "JSON.stringify", true, 0);
                _jsonStringify = new JsHandleV8(ref _jsonStringifyV8);
            }
            return _jsonStringify;
        }

        //private InternalHandle _implicitNullV8 = InternalHandle.Empty;
        //private InternalHandle _explicitNullV8 = InternalHandle.Empty;

        //private JsHandleV8 _implicitNull = JsHandleV8.Empty;
        //private JsHandleV8 _explicitNull = JsHandleV8.Empty;

        //public JsHandleV8 ImplicitNull()
        //{
        //    if (_implicitNull.IsEmpty)
        //    {
        //        var nullVal = Engine.CreateNullValue();
        //        _implicitNullV8 = nullVal;
        //        _implicitNull = new JsHandleV8() { Item = nullVal };
        //        Engine.AddToLastMemorySnapshotBefore(nullVal);
        //    }
        //    return _implicitNull;
        //}

        //public JsHandleV8 ExplicitNull()
        //{
        //    if (_explicitNull.IsEmpty)
        //    {
        //        //TODO: egor why?
        //        var nullVal = Engine.CreateNullValue();
        //        _explicitNullV8 = nullVal;
        //        _explicitNull = new JsHandleV8() { Item = nullVal };
        //        Engine.AddToLastMemorySnapshotBefore(nullVal);
        //    }
        //    return _explicitNull;
        //}


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
                    => tb.CreateObjectBinder< ObjectInstanceBaseV8.CustomBinder<TimeSeriesSegmentObjectInstanceV8>, TimeSeriesSegmentObjectInstanceV8>((TimeSeriesSegmentObjectInstanceV8)obj, initializeBinder, keepAlive: true);
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
                    => tb.CreateObjectBinder<ObjectInstanceBaseV8.CustomBinder<CounterEntryObjectInstanceV8>, CounterEntryObjectInstanceV8>((CounterEntryObjectInstanceV8)obj, initializeBinder, keepAlive: true);
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
                    => tb.CreateObjectBinder<ObjectInstanceBaseV8.CustomBinder<AttachmentNameObjectInstanceV8>, AttachmentNameObjectInstanceV8>((AttachmentNameObjectInstanceV8)obj, initializeBinder, keepAlive: true);
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
                    => tb.CreateObjectBinder<ObjectInstanceBaseV8.CustomBinder<AttachmentObjectInstanceV8>, AttachmentObjectInstanceV8>((AttachmentObjectInstanceV8)obj, initializeBinder, keepAlive: true);
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
    //TODO: egor do we need this??
    //public static void DisposeAndCollectGarbage(List<object> items, string snapshotName)
    //{
    //    V8Engine? engine = null;
    //    for (int i = items.Count - 1; i >= 0; i--)
    //    {
    //        var v8Handle = items[i] is InternalHandle ? (InternalHandle)items[i] : InternalHandle.Empty;
    //        if (!v8Handle.IsEmpty)
    //        {
    //            if (engine == null)
    //                engine = v8Handle.Engine;
    //            v8Handle.Dispose();
    //        }
    //        else
    //        {
    //            var jsHandle = items[i] is JsHandle ? (JsHandle)items[i] : JsHandle.Empty;
    //            if (!jsHandle.IsEmpty)
    //            {
    //                if (engine == null)
    //                    engine = (V8Engine)jsHandle.Engine;
    //                jsHandle.Dispose();
    //            }
    //        }
    //    }

    //    if (engine != null && engine.IsMemoryChecksOn)
    //    {
    //        engine.ForceV8GarbageCollection();
    //        engine.CheckForMemoryLeaks(snapshotName);
    //    }

    //} 

    private ContextEx? _contextEx;

    public ContextEx Context
    {
        get => _contextEx;
        set
        {
            if (_contextEx == null || !ReferenceEquals(value, _contextEx))
            {
                _contextEx = value;
                Engine.SetContext(value.ContextNative);
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
            for (int i = 0; i < arr.Length; i++)
            {
                if (arr[i] is InternalHandle jsItem)
                    jsItem.Dispose();
            }
        }
    }

    // ------------------------------------------ IJavaScriptEngineHandle implementation
    public JavaScriptEngineType EngineType => JavaScriptEngineType.V8;

    public IJavaScriptOptions? JsOptions => Context.JsOptions;
    public bool IsMemoryChecksOn { get; }
    public JsHandleV8 Empty { get; set; } = JsHandleV8.Empty;
    public JsHandleV8 Null { get; set; } = JsHandleV8.Null;
    public JsHandleV8 Undefined { get; set; } = JsHandleV8.Empty;
    public JsHandleV8 True { get; set; }
    public JsHandleV8 False { get; set; }

    // TODO: egor need to handle those like in DynamicJsNullJint?
    public JsHandleV8 ImplicitNull { get; set; } = JsHandleV8.Empty;
    public JsHandleV8 ExplicitNull { get; set; } = JsHandleV8.Empty;

    //public JsHandleV8 ImplicitNull() => Context.ImplicitNull();
    //public JsHandleV8 ExplicitNull() => Context.ExplicitNull();

    public JsHandleV8 JsonStringify() => Context.JsonStringify();

    public IDisposable DisableConstraints()
    {
        return Engine.DisableMaxDuration();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ForceGarbageCollection()
    {
        Engine.ForceV8GarbageCollection();
    }

    public object MakeSnapshot(string name)
    {
        return Engine.MakeMemorySnapshot(name);
    }

    public bool RemoveMemorySnapshot(string name)
    {
        return Engine.RemoveMemorySnapshot(name);
    }

    public void AddToLastMemorySnapshotBefore(JsHandleV8 h)
    {
        //  AddToLastMemorySnapshotBefore(h.V8.Item);
    }

    public void RemoveFromLastMemorySnapshotBefore(JsHandleV8 h)
    {
        //  RemoveFromLastMemorySnapshotBefore(h.V8.Item);
    }

    public void CheckForMemoryLeaks(string name, bool shouldRemove = true)
    {
        throw new NotImplementedException();
    }

    public void TryCompileScript(string script)
    {
        try
        {
            using (Engine.Compile(script, "script", true))
            { }
        }
        catch (Exception e)
        {
            throw new JavaScriptParseException("Failed to parse:" + Environment.NewLine + script, e);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Execute(string source, string sourceName = "anonymousCode.js", bool throwExceptionOnError = true)
    {
        Engine.Execute(source, sourceName, throwExceptionOnError);
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
        { }
    }

    new public void ExecuteWithReset(InternalHandle script, bool throwExceptionOnError = true, int timeout = 0)
    {
        using (ExecuteExprWithReset(script, throwExceptionOnError, timeout))
        { }
    }

    new public InternalHandle ExecuteExprWithReset(string source, string sourceName = "anonymousCode.js", bool throwExceptionOnError = true, int timeout = 0)
    {
        using (var script = Engine.Compile(source, sourceName, throwExceptionOnError))
        {
            return ExecuteExprWithReset(script, throwExceptionOnError, timeout);
        }
    }

    new public InternalHandle ExecuteExprWithReset(InternalHandle script, bool throwExceptionOnError = true, int timeout = 0)
    {
        try
        {
            return Engine.Execute(script, throwExceptionOnError, Engine.RefineMaxDuration(timeout));
        }
        finally
        {
            Engine.ResetCallStack();
            Engine.ResetConstraints();
        }
    }

    public JsHandleV8 GlobalObject
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            var obj = Engine.GlobalObject;
            return new JsHandleV8(ref obj);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public JsHandleV8 GetGlobalProperty(string propertyName)
    {
        var obj = Engine.GlobalObject.GetProperty(propertyName);
        return new JsHandleV8(ref obj);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetGlobalProperty(string propertyName, JsHandleV8 value)
    {
        Engine.GlobalObject.SetProperty(propertyName, value.Item);
    }

    public void ResetConstraints()
    {
        Engine.ResetConstraints();
    }

    public JsHandleV8 FromObjectGen(object obj, bool keepAlive = false)
    {
        var val = Engine.FromObject(obj, keepAlive);
        return new JsHandleV8(ref val);
    }

    public JsHandleV8 CreateClrCallBack(string propertyName, Func<JsHandleV8, JsHandleV8[], JsHandleV8> func, bool keepAlive = true)
    {
        var res = Engine.CreateClrCallBack(CallbackFunction, keepAlive);

        return new JsHandleV8(ref res);

        InternalHandle CallbackFunction(V8Engine engine, bool isconstructcall, InternalHandle _this, InternalHandle[] args)
        {
            return func(new JsHandleV8(ref _this), args.ToJsHandleArray()).Item;
        }
    }

    public void SetGlobalClrCallBack(string propertyName, Func<JsHandleV8, JsHandleV8[], JsHandleV8> funcTuple)
    {
        var jsFunc = Engine.CreateClrCallBack(CallbackFunction, true);
        if (Engine.GlobalObject.SetProperty(propertyName, jsFunc) == false)
        {
            throw new InvalidOperationException($"Failed to set global property {propertyName}");
        }

        InternalHandle CallbackFunction(V8Engine engine, bool isconstructcall, InternalHandle _this, InternalHandle[] args)
        {
            return funcTuple(new JsHandleV8(ref _this), args.ToJsHandleArray()).Item;
        }
    }

    public JsHandleV8 CreateObject()
    {
        var obj = Engine.CreateObject();
        return new JsHandleV8(ref obj);
    }

    public JsHandleV8 CreateEmptyArray()
    {
        var obj = Engine.CreateEmptyArray();
        return new JsHandleV8(ref obj);
    }

    public JsHandleV8 CreateArray(JsHandleV8[] items)
    {
        int arrayLength = items.Length;
        var jsItems = new InternalHandle[arrayLength];
        for (int i = 0; i < arrayLength; ++i)
        {
            var jhItem = items[i];
            jsItems[i] = jhItem.Item;

        }

        var obj = Engine.CreateArrayWithDisposal(jsItems);
        return new JsHandleV8(ref obj);
    }

    public JsHandleV8 CreateArray(System.Array items)
    {
        var obj = Engine.CreateArray(items);
        return new JsHandleV8(ref obj);
    }

    public JsHandleV8 CreateArray(IEnumerable<object> items)
    {
        var obj = Engine.CreateArray(items);
        return new JsHandleV8(ref obj);
    }

    public JsHandleV8 CreateArray(IEnumerable<JsHandleV8> items)
    {
        var empty = true;
        List<InternalHandle> jsValues = new List<InternalHandle>();
        foreach (var item in items)
        {
            empty = false;
            jsValues.Add(item.Item);
        }
        if (empty)
            return CreateEmptyArray();
        var obj = Engine.CreateArray(jsValues.ToArray());
        return new JsHandleV8(ref obj);
    }

    public JsHandleV8 CreateUndefinedValue()
    {
        // TODO: egor is it right?
        return JsHandleV8.Empty;
    }

    public JsHandleV8 CreateNullValue()
    {
        var obj = Engine.CreateNullValue();
        return new JsHandleV8(ref obj);
    }

    public JsHandleV8 CreateValue(bool value)
    {
        var obj = Engine.CreateValue(value);
        return new JsHandleV8(ref obj);
    }

    public JsHandleV8 CreateValue(Int32 value)
    {
        var obj = Engine.CreateValue(value);
        return new JsHandleV8(ref obj);
    }

    public JsHandleV8 CreateValue(double value)
    {
        var obj = Engine.CreateValue(value);
        return new JsHandleV8(ref obj);
    }

    public JsHandleV8 CreateValue(long value)
    {
        var obj = Engine.CreateValue(value);
        return new JsHandleV8(ref obj);
    }

    public JsHandleV8 CreateValue(string value)
    {
        var obj = Engine.CreateValue(value);
        return new JsHandleV8(ref obj);
    }

    public JsHandleV8 CreateValue(TimeSpan ms)
    {
        var obj = Engine.CreateValue(ms);
        return new JsHandleV8(ref obj);
    }

    public JsHandleV8 CreateValue(DateTime value)
    {
        var obj = Engine.CreateValue(value);
        return new JsHandleV8(ref obj);
    }

    public JsHandleV8 CreateError(string message, JSValueType errorType)
    {
        var obj = Engine.CreateError(message, errorType);
        return new JsHandleV8(ref obj);
    }

    // ------------------------------------------ internal implementation
    public readonly V8Engine Engine;
    public V8EngineEx()
    {
        Engine = new V8Engine(false, jsConverter: JsConverterInstance);
        var trueVal = Engine.CreateValue(true);
        var falseVal = Engine.CreateValue(true);
        True = new JsHandleV8(ref trueVal);
        False = new JsHandleV8(ref falseVal);
    }

    public ContextEx CreateAndSetContextEx(RavenConfiguration configuration, IJavaScriptContext jsContext, ObjectTemplate? globalTemplate = null)
    {
        var contextEx = new ContextEx(Engine, jsContext, globalTemplate);
        Context = contextEx;
        contextEx.SetOptions(configuration);
        contextEx.InitializeGlobal();
        return contextEx;
    }

    public IDisposable ChangeMaxStatements(int maxDurationNew)
    {
        // doing nothing as V8 doesn't support limiting MaxStatements

        return (IDisposable)null;
    }

    public IDisposable ChangeMaxDuration(int value)
    {
        return Engine.ChangeMaxDuration(value);
    }

    public void ResetCallStack()
    {
        Engine.ResetCallStack();
    }

    public void Dispose()
    {
        Context?.Dispose();
        Engine.Dispose();
    }
}
