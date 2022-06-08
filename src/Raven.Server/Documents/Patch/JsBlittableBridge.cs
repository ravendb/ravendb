using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Jint.Runtime.Interop;
using Raven.Client;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Utils;
using Voron.Util;

namespace Raven.Server.Documents.Patch;

public abstract class JsBlittableBridge<T>
    where T : struct, IJsHandle<T>
{
    protected  ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> _writer;
    protected BlittableJsonDocumentBuilder.UsageMode _usageMode;
    public IJsEngineHandle<T> _scriptEngine;
    [ThreadStatic]
    protected static HashSet<object> _recursive;


    protected static readonly double MaxJsDateMs = (DateTime.MaxValue - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;
    protected static readonly double MinJsDateMs = -(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc) - DateTime.MinValue).TotalMilliseconds;

    static JsBlittableBridge()
    {
        ThreadLocalCleanup.ReleaseThreadLocalState += () => _recursive = null;
    }

    protected JsBlittableBridge(IJsEngineHandle<T> scriptEngine)
    {
        _scriptEngine = scriptEngine;
    }

    public IDisposable Initialize(ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> writer, BlittableJsonDocumentBuilder.UsageMode usageMode)
    {
        _writer = writer;
        _usageMode = usageMode;

        return new DisposableAction(() =>
        {
            _writer = null;
            _usageMode = default;
        });
    }

    protected void WriteInstance(T jsObject, IResultModifier modifier, bool isRoot, bool filterProperties)
    {
        _writer.StartWriteObject();

        modifier?.Modify(jsObject, _scriptEngine);
        var boundObject = jsObject.AsObject();
        if (boundObject is IBlittableObjectInstance<T> boi)
        {
            WriteBlittableInstance(boi, isRoot, filterProperties);
        }
        else
            WriteJsInstance(jsObject, isRoot, filterProperties);

        _writer.WriteObjectEnd();
    }

    protected abstract string GetDateValue(T jsValue, string propertyName);

    protected void WriteJsonValue(object jsParent, bool isRoot, bool filterProperties, string propertyName, T jsValue)
    {
        jsValue.ThrowOnError();

        if (jsValue.IsBoolean)
            _writer.WriteValue(jsValue.AsBoolean);
        else if (jsValue.IsUndefined || jsValue.IsNull)
            _writer.WriteValueNull();
        else if (jsValue.IsStringEx)
            _writer.WriteValue(jsValue.AsString);
        else if (jsValue.IsDate)
            _writer.WriteValue(GetDateValue(jsValue, propertyName));
        else if (jsValue.IsInt32)
            WriteNumber(jsParent, propertyName, jsValue.AsInt32);
        else if (jsValue.IsNumberEx)
            WriteNumber(jsParent, propertyName, jsValue.AsDouble);
        else if (jsValue.IsArray)
            WriteArray(jsValue);
        else if (jsValue.IsObject)
        {
            if (isRoot)
                filterProperties = string.Equals(propertyName, Constants.Documents.Metadata.Key, StringComparison.Ordinal);

            object asObject = jsValue.AsObject();
            if (asObject is ObjectWrapper wrapper)
            {

                if (wrapper.Target is LazyNumberValue)
                {
                    _writer.WriteValue(BlittableJsonToken.LazyNumber, wrapper.Target);
                }
                else if (wrapper.Target is LazyStringValue)
                {
                    _writer.WriteValue(BlittableJsonToken.String, wrapper.Target);
                }
                else if (wrapper.Target is LazyCompressedStringValue)
                {
                    _writer.WriteValue(BlittableJsonToken.CompressedString, wrapper.Target);
                }
                else if (wrapper.Target is long)
                {
                    _writer.WriteValue(BlittableJsonToken.Integer, (long)wrapper.Target);
                }
                else
                {
                    WriteNestedObject(jsValue, filterProperties);
                }
            }
            else if (asObject != null)
            {
                //TODO: egor in other places we use the Item.BoundObject for v8, should we do here  as well? or asObject = BOundObject in V8 so I can drop usage of Item.BoundObject and just use asObj is LazyNum... like here also in other places
                if (asObject is LazyNumberValue)
                {
                    _writer.WriteValue(BlittableJsonToken.LazyNumber, asObject);
                }
                else if (asObject is LazyStringValue)
                {
                    _writer.WriteValue(BlittableJsonToken.String, asObject);
                }
                else if (asObject is LazyCompressedStringValue)
                {
                    _writer.WriteValue(BlittableJsonToken.CompressedString, asObject);
                }
                else if (asObject is long)
                {
                    _writer.WriteValue(BlittableJsonToken.Integer, (long)asObject);
                }
                else
                {
                    WriteNestedObject(jsValue, filterProperties);
                }
            }
            else
            {
                WriteNestedObject(jsValue, filterProperties);
            }
        }
    }

    private void WriteArray(T jsArr)
    {
        _writer.StartWriteArray();
        for (int i = 0; i < jsArr.ArrayLength; i++)
        {
            using (var jsValue = jsArr.GetProperty(i))
            {
                WriteJsonValue(jsArr, false, false, i.ToString(), jsValue);
            }
        }
        _writer.WriteArrayEnd();
    }

    private void WriteValue(object parent, bool isRoot, string propertyName, object value)
    {
        if (value is bool b)
            _writer.WriteValue(b);
        else if (value is string s)
            _writer.WriteValue(s);
        else if (value is byte by)
            _writer.WriteValue(by);
        else if (value is int n)
            WriteNumber(parent, propertyName, n);
        else if (value is uint ui)
            _writer.WriteValue(ui);
        else if (value is long l)
            _writer.WriteValue(l);
        else if (value is double d)
        {
            WriteNumber(parent, propertyName, d);
        }
        else if (value == null)
            _writer.WriteValueNull();
        else if (value is LazyStringValue lsv)
        {
            _writer.WriteValue(lsv);
        }
        else if (value is LazyCompressedStringValue lcsv)
        {
            _writer.WriteValue(lcsv);
        }
        else if (value is LazyNumberValue lnv)
        {
            _writer.WriteValue(lnv);
        }
        else
        {
            throw new NotSupportedException(value.GetType().ToString());
        }
    }

    protected abstract void WriteNestedObject(T jsObj, bool filterProperties);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected void WriteObjectType(object target)
    {
        _writer.WriteValue('[' + target.GetType().Name + ']');
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public abstract void WriteValueInternal(object target, T jsObj, bool filterProperties);

    private void WriteNumber(object parent, string propName, double d)
    {
         var writer = _writer;
            var boi = parent as IBlittableObjectInstance<T>;
            if (boi == null || propName == null)
            {
                GuessNumberType();
                return;
            }

            if (boi.OriginalPropertiesTypes != null &&
                boi.OriginalPropertiesTypes.TryGetValue(propName, out var numType))
            {
                if (WriteNumberBasedOnType(numType & BlittableJsonReaderBase.TypesMask))
                    return;
            }
            else if (boi.Blittable != null)
            {
                var propIndex = boi.Blittable.GetPropertyIndex(propName);
                if (propIndex != -1)
                {
                    var prop = new BlittableJsonReaderObject.PropertyDetails();
                    boi.Blittable.GetPropertyByIndex(propIndex, ref prop);
                    if (WriteNumberBasedOnType(prop.Token & BlittableJsonReaderBase.TypesMask))
                        return;
                }
            }

            GuessNumberType();

            bool WriteNumberBasedOnType(BlittableJsonToken type)
            {
                if (type == BlittableJsonToken.LazyNumber)
                {
                    writer.WriteValue(d);
                    return true;
                }

                if (type == BlittableJsonToken.Integer)
                {
                    if (IsDoubleType())
                    {
                        // the previous value was a long and now changed to double
                        writer.WriteValue(d);
                    }
                    else
                    {
                        writer.WriteValue((long)d);
                    }

                    return true;
                }

                return false;
            }

            void GuessNumberType()
            {
                if (IsDoubleType())
                {
                    writer.WriteValue(d);
                }
                else
                {
                    writer.WriteValue((long)d);
                }
            }

            bool IsDoubleType()
            {
                var roundedNumber = Math.Round(d, 0);
                if (roundedNumber.AlmostEquals(d))
                {
                    var digitsAfterDecimalPoint = Math.Abs(roundedNumber - d);
                    if (digitsAfterDecimalPoint == 0 && Math.Abs(roundedNumber) <= long.MaxValue)
                        return false;
                }

                return true;
            }
    }

    private void WriteJsInstance(T instance, bool isRoot, bool filterProperties)
    {
        IEnumerable<KeyValuePair<string, T>> properties;
        if (instance.IsBinder())
        {
            properties = instance.GetOwnProperties();
        }
        else
        {

            properties = instance.GetOwnProperties();
        }

        foreach (var (propertyName, jsPropertyValue) in properties)
        {
            using (jsPropertyValue)
            {
                if (ShouldFilterProperty(filterProperties, propertyName))
                    continue;

                if (jsPropertyValue.IsEmpty)
                    continue;

                _writer.WritePropertyName(propertyName);

                WriteJsonValue(instance, isRoot, filterProperties, propertyName, jsPropertyValue);
            }
        }
    }

    protected abstract unsafe void WriteBlittableInstance(IBlittableObjectInstance<T> jsObj, bool isRoot, bool filterProperties);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected static bool ShouldFilterProperty(bool filterProperties, string property)
    {
        if (filterProperties == false)
            return false;

        return property == Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName ||
               property == Constants.Documents.Indexing.Fields.DocumentIdFieldName ||
               property == Constants.Documents.Indexing.Fields.SourceDocumentIdFieldName ||
               property == Constants.Documents.Metadata.Id ||
               property == Constants.Documents.Metadata.LastModified ||
               property == Constants.Documents.Metadata.IndexScore ||
               property == Constants.Documents.Metadata.ChangeVector ||
               property == Constants.Documents.Metadata.Flags;
    }

    public BlittableJsonReaderObject Translate(JsonOperationContext context, T instance,
        IResultModifier modifier = null, BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None, bool isRoot = true)
    {
        var objectInstance = instance.AsObject();
        if (objectInstance == null)
            return null;

        if (objectInstance is IBlittableObjectInstance<T> boi && boi.Changed == false && isRoot)
            return boi.Blittable.Clone(context);

        using (var writer = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
        using (Initialize(writer, usageMode))
        {
            writer.Reset(usageMode);
            writer.StartWriteObjectDocument();
            WriteInstance(instance, modifier, isRoot: isRoot, filterProperties: false);

            writer.FinalizeDocument();

            return writer.CreateReader();
        }
    }
}

public interface IBlittableObjectProperty<T> : IDisposable
    where T : struct, IJsHandle<T>
{
    bool Changed { get; }
    T ValueHandle { get; }
}

//public interface IBlittableObjectProperty : IDisposable
//{
//    bool Changed { get; }
//    T ValueHandle { get; set; }
//}

public interface IResultModifier
{
    void Modify<T>(T json, IJsEngineHandle<T> engine) where T : struct, IJsHandle<T>;
}
