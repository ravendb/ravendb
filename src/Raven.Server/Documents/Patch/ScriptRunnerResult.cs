using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Documents.Patch.V8;
using Sparrow.Json;

namespace Raven.Server.Documents.Patch;

public abstract class ScriptRunnerResult<T> : IScriptRunnerResult
    where T : struct, IJsHandle<T>
{
    private readonly SingleRun<T> _parent;
    public T Instance;

    protected ScriptRunnerResult(SingleRun<T> parent, T instance)
    {
        _parent = parent;
        Instance = instance.Clone();
    }

    ~ScriptRunnerResult()
    {
        Instance.Dispose();
    }

    public IJsEngineHandle<T> EngineHandle => _parent.ScriptEngineHandle;
    public abstract bool GetOrCreateInternal(string propertyName, out T value);
    public abstract void TryReset();
    public T GetOrCreate(string propertyName)
    {
        //TODO: egor (needed for ETL only)
        if (GetOrCreateInternal(propertyName, out T value))
            return value;

        T o = Instance.GetProperty(propertyName);
        if (o.IsUndefined || o.IsNull)
        {
            o.Dispose();
            o = _parent.ScriptEngineHandle.CreateObject();
            Instance.SetProperty(propertyName, o, throwOnError: true);
        }
        return o;
    }

    public bool? BooleanValue
    {
        get => Instance.IsBoolean ? Instance.AsBoolean : (bool?)null;
    }

    public bool IsNull
    {
        get => Instance.IsEmpty || Instance.IsNull || Instance.IsUndefined;
    }

    public string StringValue => Instance.IsStringEx ? Instance.AsString : null;
    public T RawJsValue => Instance;
    //public object RawJsValue
    //{
    //    get => Instance;
    //    set => Instance = value;
    //}

    public BlittableJsonReaderObject TranslateToObject(JsonOperationContext context, IResultModifier modifier = null, 
        BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None)
    {
        if (IsNull)
            return null;

        return _parent.JsBlittableBridge.Translate(context, Instance, modifier, usageMode);
    }

    public  void Dispose()
    {
        TryReset();


        _parent?.JsUtils.Clear();
    }

    public object TranslateRawJsValue(JsonOperationContext context, IResultModifier modifier = null,
        BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None, bool isRoot = true)
    {
        var jsValue = TranslateRawJsValueInternal(RawJsValue, context, modifier, usageMode, isRoot);
        return jsValue;
    }

    private object TranslateRawJsValueInternal(T jsValue, JsonOperationContext context, IResultModifier modifier = null,
        BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None, bool isRoot = true)
    {
        if (jsValue.IsStringEx)
            return jsValue.AsString;
        if (jsValue.IsBoolean)
            return jsValue.AsBoolean;
        if (jsValue.IsArray)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            var list = new List<object>();
            for (int i = 0; i < jsValue.ArrayLength; i++)
            {
                using (var jsItem = jsValue.GetProperty(i))
                {
                    list.Add(TranslateRawJsValueInternal(jsItem, context, modifier, usageMode, isRoot));
                }
            }

            return list;
        }

        if (jsValue.IsObject)
        {
            if (jsValue.IsNull)
                return null;

            return _parent.JsBlittableBridge.Translate(context, Instance, modifier, usageMode);
        }

        if (jsValue.IsNumberOrIntEx)
            return jsValue.AsDouble;
        if (jsValue.IsNull || jsValue.IsUndefined)
            return null;
        throw new NotSupportedException("Unable to translate " + jsValue.ValueType);
    }
}

public class ScriptRunnerResultJint : ScriptRunnerResult<JsHandleJint>
{
    public ScriptRunnerResultJint(SingleRun<JsHandleJint> parent, JsHandleJint instance) : base(parent, instance)
    {
    }

    public override bool GetOrCreateInternal(string propertyName, out JsHandleJint value)
    {
        value = default;
        if (Instance.Object is BlittableObjectInstanceJint boi)
        {
           value = boi.GetOrCreate(propertyName);
           return true;
        }

        return false;
    }

    public override void TryReset()
    {
        if (Instance.IsObject && Instance.Object is BlittableObjectInstanceJint boi)
        {
            boi.Reset();
        }
    }
}

public class ScriptRunnerResultV8 : ScriptRunnerResult<JsHandleV8>
{
    public ScriptRunnerResultV8(SingleRun<JsHandleV8> parent, JsHandleV8 instance) : base(parent, instance)
    {
    }

    public override bool GetOrCreateInternal(string propertyName, out JsHandleV8 value)
    {
        //TODO: egor make it nullabel???
        value = default;
        if (Instance.Object is BlittableObjectInstanceV8 boi)
        {
            value = boi.GetOrCreate(propertyName);
            return true;
        }

        return false;
    }

    public override void TryReset()
    {
        if (Instance.IsObject && Instance.Object is BlittableObjectInstanceV8 boi)
        {
            boi.Reset();
        }
    }
}

public interface IScriptRunnerResult : IDisposable
{
    object TranslateRawJsValue(JsonOperationContext context, IResultModifier modifier = null,
        BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None, bool isRoot = true);

    BlittableJsonReaderObject TranslateToObject(JsonOperationContext context, IResultModifier modifier = null,
        BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None);
    public bool? BooleanValue { get; }
    public bool IsNull { get; }
    public string StringValue { get; }
}
