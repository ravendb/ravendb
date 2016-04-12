using System;
using System.Collections.Generic;
using System.Text;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime;
using Jint.Runtime.Descriptors;
using Raven.Abstractions.Data;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Patch
{
    public class PatcherOperationScope : IDisposable
    {
        private readonly DocumentDatabase _database;
        private readonly DocumentsOperationContext _context;
        private readonly Dictionary<string, KeyValuePair<object, JsValue>> _propertiesByValue = new Dictionary<string, KeyValuePair<object, JsValue>>();

        public readonly DynamicJsonArray DebugInfo = new DynamicJsonArray();

        private static readonly List<string> InheritedProperties = new List<string>
        {
            "length",
            "Map",
            "Where",
            "RemoveWhere",
            "Remove"
        };

        public bool DebugMode { get; }

        public readonly PatchDebugActions DebugActions;

        public string CustomFunctions { get; set; }

        public int AdditionalStepsPerSize { get; set; }
        public int MaxSteps { get; set; }

        public JsValue ActualPatchResult { get; set; }
        public JsValue PatchObject;

        public PatcherOperationScope(DocumentDatabase database, DocumentsOperationContext context, bool debugMode = false)
        {
            _database = database;
            _context = context;
            DebugMode = debugMode;
            if (DebugMode)
            {
                DebugActions = new PatchDebugActions();
            }
        }

        public JsValue ToJsArray(Engine engine, BlittableJsonReaderArray json, string propertyKey)
        {
            var result = engine.Array.Construct(Arguments.Empty);
            for (var i = 0; i < json.Length; i++)
            {
                var value = json.GetValueTokenTupleByIndex(i);
                var index = i.ToString();
                var jsVal = ToJsValue(engine, value.Item1, value.Item2, propertyKey + "[" + index + "]");
                result.FastAddProperty(index, jsVal, true, true, true);
            }
            result.FastSetProperty("length", new PropertyDescriptor
            {
                Value = new JsValue(json.Length),
                Configurable = true,
                Enumerable = true,
                Writable = true,
            });
            return result;
        }

        public ObjectInstance ToJsObject(Engine engine, BlittableJsonReaderObject json, string propertyName = null)
        {
            var jsObject = engine.Object.Construct(Arguments.Empty);
            for (int i = 0; i < json.Count; i++)
            {
                var property = json.GetPropertyByIndex(i);
                var name = property.Item1.ToString();
                var propertyKey = CreatePropertyKey(name, propertyName);
                var value = property.Item2;
                JsValue jsValue = ToJsValue(engine, value, property.Item3, propertyKey);
                _propertiesByValue[propertyKey] = new KeyValuePair<object, JsValue>(value, jsValue);
                jsObject.FastAddProperty(name, jsValue, true, true, true);
            }
            return jsObject;
        }

        public JsValue ToJsValue(Engine engine, object value, BlittableJsonToken token, string propertyKey = null)
        {
            switch (token & BlittableJsonReaderBase.TypesMask)
            {
                case BlittableJsonToken.Null:
                    return JsValue.Null;
                case BlittableJsonToken.Boolean:
                    return new JsValue((bool) value);

                case BlittableJsonToken.Integer:
                    return new JsValue((long) value);
                case BlittableJsonToken.Float:
                    return new JsValue((double) (LazyDoubleValue) value);
                case BlittableJsonToken.String:
                    return new JsValue(((LazyStringValue) value).ToString());
                case BlittableJsonToken.CompressedString:
                    return new JsValue(((LazyCompressedStringValue) value).ToString());

                case BlittableJsonToken.StartObject:
                    return ToJsObject(engine, (BlittableJsonReaderObject) value, propertyKey);
                case BlittableJsonToken.StartArray:
                    return ToJsArray(engine, (BlittableJsonReaderArray) value, propertyKey);

                default:
                    throw new ArgumentOutOfRangeException(token.ToString());
            }
        }

        private static string CreatePropertyKey(string key, string property)
        {
            if (string.IsNullOrEmpty(property))
                return key;

            return property + "." + key;
        }

        public DynamicJsonValue ToBlittable(ObjectInstance jsObject, string propertyKey = null, bool recursiveCall = false)
        {
            if (jsObject.Class == "Function")
            {
                // getting a Function instance here,
                // means that we couldn't evaluate it using Jint
                return null;
            }

            var obj = new DynamicJsonValue();
            foreach (var property in jsObject.GetOwnProperties())
            {
                if (property.Key == Constants.ReduceKeyFieldName || property.Key == Constants.DocumentIdFieldName)
                    continue;

                var value = property.Value.Value;
                if (value.HasValue == false)
                    continue;

                if (value.Value.IsRegExp())
                    continue;

                var recursive = jsObject == value;
                if (recursiveCall && recursive)
                    obj[property.Key] = null;
                else
                    obj[property.Key] = ToBlittableValue(value.Value, CreatePropertyKey(property.Key, propertyKey), recursive);
            }
            return obj;
        }

        private object ToBlittableValue(JsValue v, string propertyKey, bool recursiveCall)
        {
            if (v.IsBoolean())
                return v.AsBoolean();

            if (v.IsString())
            {
                const string RavenDataByteArrayToBase64 = "raven-data:byte[];base64,";
                var valueAsObject = v.ToObject();
                var value = valueAsObject?.ToString();
                if (value != null && value.StartsWith(RavenDataByteArrayToBase64))
                {
                    value = value.Remove(0, RavenDataByteArrayToBase64.Length);
                    var byteArray = Convert.FromBase64String(value);
                    return Encoding.UTF8.GetString(byteArray);
                }
                return value;
            }

            if (v.IsNumber())
            {
                var num = v.AsNumber();

                KeyValuePair<object, JsValue> property;
                if (_propertiesByValue.TryGetValue(propertyKey, out property))
                {
                    var originalValue = property.Key;
                    if (originalValue is float || originalValue is int)
                    {
                        // If the current value is exactly as the original value, we can return the original value before we made the JS conversion, 
                        // which will convert a Int64 to jsFloat.
                        var jsValue = property.Value;
                        if (jsValue.IsNumber() && Math.Abs(num - jsValue.AsNumber()) < double.Epsilon)
                            return originalValue;

                        //We might have change the type of num from Integer to long in the script by design 
                        //Making sure the number isn't a real float before returning it as integer
                        if (originalValue is int && (Math.Abs(num - Math.Floor(num)) <= double.Epsilon || Math.Abs(num - Math.Ceiling(num)) <= double.Epsilon))
                            return (long)num;
                        return num; //float
                    }
                }

                // If we don't have the type, assume that if the number ending with ".0" it actually an integer.
                var integer = Math.Truncate(num);
                if (Math.Abs(num - integer) < double.Epsilon)
                    return (long) integer;
                return num;
            }
            if (v.IsNull() || v.IsUndefined())
                return null;
            if (v.IsArray())
            {
                var jsArray = v.AsArray();
                var array = new DynamicJsonArray();

                foreach (var property in jsArray.GetOwnProperties())
                {
                    if (InheritedProperties.Contains(property.Key))
                        continue;

                    var jsInstance = property.Value.Value;
                    if (!jsInstance.HasValue)
                        continue;

                    var ravenJToken = ToBlittableValue(jsInstance.Value, propertyKey + "[" + property.Key + "]", recursiveCall);
                    if (ravenJToken == null)
                        continue;

                    array.Add(ravenJToken);
                }

                return array;
            }
            if (v.IsDate())
            {
                return v.AsDate().ToDateTime();
            }
            if (v.IsObject())
            {
                return ToBlittable(v.AsObject(), propertyKey, recursiveCall);
            }
            if (v.IsRegExp())
                return null;

            throw new NotSupportedException(v.Type.ToString());
        }

        public void Dispose()
        {
        }

        public virtual JsValue LoadDocument(string documentKey, Engine engine, ref int totalStatements)
        {
            var document = _database.DocumentsStorage.Get(_context, documentKey);

            if (DebugMode)
                DebugActions.LoadDocument.Add(documentKey);

            if (document == null)
                return JsValue.Null;

            totalStatements += (MaxSteps/2 + (document.Data.Size*AdditionalStepsPerSize));
            engine.Options.MaxStatements(totalStatements);

            // TODO: Make sure to add Constants.DocumentIdFieldName to document.Data
            return ToJsObject(engine, document.Data);
        }

        public virtual string PutDocument(string key, JsValue document, JsValue metadata, JsValue etagJs, Engine engine)
        {
            if (document.IsObject() == false)
            {
                throw new InvalidOperationException(
                    $"Created document must be a valid object which is not null or empty. Document key: '{key}'.");
            }

            long? etag = null;
            if (etagJs.IsNumber())
            {
                etag = (long) etagJs.AsNumber();
            }
            else if(etagJs.IsNull() == false && etagJs.IsUndefined() == false && etagJs.ToString() != "None")
            {
                throw new InvalidOperationException($"Invalid ETag value for document '{key}'");
            }

            var data = ToBlittable(document.AsObject());
            if (metadata.IsObject())
            {
                data["@metadata"] = ToBlittable(metadata.AsObject());
            }

            if (DebugMode)
            {
                DebugActions.PutDocument.Add(new DynamicJsonValue
                {
                    ["Key"] = key,
                    ["Etag"] = etag,
                    ["Data"] = data,
                });
            }

            var dataReader = _context.ReadObject(data, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            var put = _database.DocumentsStorage.Put(_context, key, etag, dataReader);

            return put.Key;
        }

        public virtual void DeleteDocument(string documentKey)
        {
            throw new NotSupportedException("Deleting documents is not supported.");
        }
    }
}