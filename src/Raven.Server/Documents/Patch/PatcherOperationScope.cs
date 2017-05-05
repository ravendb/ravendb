using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime;
using Jint.Runtime.Descriptors;
using Raven.Client;
using Raven.Server.ServerWide.Context;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Patch
{
    public class PatcherOperationScope : IDisposable
    {
        private readonly DocumentDatabase _database;
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

        private DocumentsOperationContext _context;

        public bool DebugMode { get; }

        public readonly PatchDebugActions DebugActions;

        public string CustomFunctions { get; set; }

        public int AdditionalStepsPerSize { get; set; }

        public int MaxSteps { get; set; }

        public int TotalScriptSteps;

        public JsValue ActualPatchResult { get; set; }
        public JsValue PatchObject;

        public PatcherOperationScope(DocumentDatabase database, bool debugMode = false)
        {
            _database = database;
            DebugMode = debugMode;
            if (DebugMode)
            {
                DebugActions = new PatchDebugActions();
            }
        }

        public PatcherOperationScope Initialize(DocumentsOperationContext context)
        {
            _context = context;

            return this;
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

        public ObjectInstance ToJsObject(Engine engine, Document document)
        {
            var instance = ToJsObject(engine, document.Data);
            return ApplyMetadataIfNecessary(instance, document.Key, document.Etag, document.LastModified, document.Flags, document.IndexScore);
        }

        public ObjectInstance ToJsObject(Engine engine, DocumentConflict document, string propertyName)
        {
            var instance = ToJsObject(engine, document.Doc, propertyName);
            return ApplyMetadataIfNecessary(instance, document.Key, document.Etag, document.LastModified, flags: null, indexScore: null);
        }

        private static ObjectInstance ApplyMetadataIfNecessary(ObjectInstance instance, LazyStringValue key, long etag, DateTime? lastModified, DocumentFlags? flags, double? indexScore)
        {
            var metadataValue = instance.Get(Constants.Documents.Metadata.Key);
            if (metadataValue == null || metadataValue.IsObject() == false)
                return instance;

            var metadata = metadataValue.AsObject();

            if (etag > 0)
                metadata.FastAddProperty(Constants.Documents.Metadata.Etag, etag, true, true, true);

            if (lastModified.HasValue && lastModified != default(DateTime))
                metadata.FastAddProperty(Constants.Documents.Metadata.LastModified, lastModified.Value.GetDefaultRavenFormat(), true, true, true);

            if (flags.HasValue && flags != DocumentFlags.None)
                metadata.FastAddProperty(Constants.Documents.Metadata.Flags, flags.Value.ToString(), true, true, true);

            if (key != null)
                metadata.FastAddProperty(Constants.Documents.Metadata.Id, key.ToString(), true, true, true);

            if (indexScore.HasValue)
                metadata.FastAddProperty(Constants.Documents.Metadata.IndexScore, indexScore, true, true, true);

            // TOOD: Do we want to expose here also the change vector?

            return instance;
        }

        private ObjectInstance ToJsObject(Engine engine, BlittableJsonReaderObject json, string propertyName = null)
        {
            var jsObject = engine.Object.Construct(Arguments.Empty);
            var prop = new BlittableJsonReaderObject.PropertyDetails();

            for (int i = 0; i < json.Count; i++)
            {
                json.GetPropertyByIndex(i, ref prop);
                var name = prop.Name.ToString();
                var propertyKey = CreatePropertyKey(name, propertyName);
                var value = prop.Value;
                JsValue jsValue = ToJsValue(engine, value, prop.Token, propertyKey);
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
                    return new JsValue((bool)value);

                case BlittableJsonToken.Integer:
                    return new JsValue((long)value);
                case BlittableJsonToken.Float:
                    return new JsValue((double)(LazyDoubleValue)value);
                case BlittableJsonToken.String:
                    return new JsValue(((LazyStringValue)value).ToString());
                case BlittableJsonToken.CompressedString:
                    return new JsValue(((LazyCompressedStringValue)value).ToString());

                case BlittableJsonToken.StartObject:
                    return ToJsObject(engine, (BlittableJsonReaderObject)value, propertyKey);
                case BlittableJsonToken.StartArray:
                    return ToJsArray(engine, (BlittableJsonReaderArray)value, propertyKey);

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

        public void ToBlittableJsonReaderObject(ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> writer, ObjectInstance jsObject, string propertyKey = null,
            bool recursiveCall = false)
        {
            if (jsObject.Class == "Function")
            {
                // getting a Function instance here,
                // means that we couldn't evaluate it using Jint
                return;
            }
            writer.StartWriteObject();
            WriteRawObjectPropertiesToBlittable(writer, jsObject, propertyKey, recursiveCall);
            writer.WriteObjectEnd();
        }

        public void WriteRawObjectPropertiesToBlittable(ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> writer, ObjectInstance jsObject, string propertyKey = null,
            bool recursiveCall = false)
        {
            foreach (var property in jsObject.GetOwnProperties())
            {
                if (ShouldFilterProperty(property.Key))
                    continue;

                var value = property.Value.Value;
                if (value == null)
                    continue;

                if (value.IsRegExp())
                    continue;

                var recursive = jsObject == value;
                writer.WritePropertyName(property.Key);
                if (recursiveCall && recursive)
                    writer.WriteValueNull();
                else
                {
                    ToBlittableJsonReaderValue(writer, value, CreatePropertyKey(property.Key, propertyKey), recursive);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool ShouldFilterProperty(string property)
        {
            return property == Constants.Documents.Indexing.Fields.ReduceKeyFieldName ||
                   property == Constants.Documents.Indexing.Fields.DocumentIdFieldName ||
                   property == Constants.Documents.Metadata.Id ||
                   property == Constants.Documents.Metadata.Etag ||
                   property == Constants.Documents.Metadata.LastModified ||
                   property == Constants.Documents.Metadata.IndexScore ||
                   property == Constants.Documents.Metadata.ChangeVector ||
                   property == Constants.Documents.Metadata.Flags;
        }

        private void ToBlittableJsonReaderValue(ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> writer, JsValue v, string propertyKey, bool recursiveCall)
        {
            if (v.IsBoolean())
            {
                writer.WriteValue(v.AsBoolean());
                return;
            }

            if (v.IsString())
            {
                const string RavenDataByteArrayToBase64 = "raven-data:byte[];base64,";
                var valueAsObject = v.ToObject();
                var value = valueAsObject?.ToString();
                if (value != null && value.StartsWith(RavenDataByteArrayToBase64))
                {
                    value = value.Remove(0, RavenDataByteArrayToBase64.Length);
                    var byteArray = Convert.FromBase64String(value);
                    writer.WriteValue(Encoding.UTF8.GetString(byteArray));
                    return;
                }
                writer.WriteValue(value);
                return;
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
                        {
                            writer.WriteValue((int)originalValue);
                            return;
                        }

                        //We might have change the type of num from Integer to long in the script by design 
                        //Making sure the number isn't a real float before returning it as integer
                        if (originalValue is int &&
                            (Math.Abs(num - Math.Floor(num)) <= double.Epsilon ||
                             Math.Abs(num - Math.Ceiling(num)) <= double.Epsilon))
                        {
                            writer.WriteValue((long)num);
                            return;
                        }
                        writer.WriteValue((float)num);
                        return; //float
                    }
                }

                // If we don't have the type, assume that if the number ending with ".0" it actually an integer.
                var integer = Math.Truncate(num);
                if (Math.Abs(num - integer) < double.Epsilon)
                {
                    writer.WriteValue((long)integer);
                    return;
                }
                writer.WriteValue(num);
                return;
            }
            if (v.IsNull() || v.IsUndefined())
            {
                writer.WriteValueNull();
                return;
            }
            if (v.IsArray())
            {
                var jsArray = v.AsArray();
                writer.StartWriteArray();
                foreach (var property in jsArray.GetOwnProperties())
                {
                    if (InheritedProperties.Contains(property.Key))
                        continue;

                    var jsInstance = property.Value.Value;
                    if (jsInstance == null)
                        continue;

                    ToBlittableJsonReaderValue(writer, jsInstance, propertyKey + "[" + property.Key + "]",
                        recursiveCall);
                }
                writer.WriteArrayEnd();
                return;
            }
            if (v.IsDate())
            {
                writer.WriteValue(v.AsDate().ToDateTime().ToString(Default.DateTimeFormatsToWrite));
                return;
            }
            if (v.IsObject())
            {
                ToBlittableJsonReaderObject(writer, v.AsObject(), propertyKey, recursiveCall);
                return;
            }
            if (v.IsRegExp())
            {
                writer.WriteValueNull();
                return;
            }

            throw new NotSupportedException(v.Type.ToString());
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
                if (ShouldFilterProperty(property.Key))
                    continue;

                var value = property.Value.Value;
                if (value == null)
                    continue;

                if (value.IsRegExp())
                    continue;

                var recursive = jsObject == value;
                if (recursiveCall && recursive)
                    obj[property.Key] = null;
                else
                    obj[property.Key] = ToBlittableValue(value, CreatePropertyKey(property.Key, propertyKey), recursive);
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
                    return (long)integer;
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
                    if (jsInstance == null)
                        continue;

                    var ravenJToken = ToBlittableValue(jsInstance, propertyKey + "[" + property.Key + "]", recursiveCall);
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
            if (_context == null)
                ThrowDocumentsOperationContextIsNotSet();

            var document = _database.DocumentsStorage.Get(_context, documentKey);

            if (DebugMode)
                DebugActions.LoadDocument.Add(documentKey);

            if (document == null)
                return JsValue.Null;

            totalStatements += (MaxSteps / 2 + (document.Data.Size * AdditionalStepsPerSize));
            engine.Options.MaxStatements(totalStatements);

            // TODO: Make sure to add Constants.Indexing.Fields.DocumentIdFieldName to document.Data
            return ToJsObject(engine, document.Data);
        }

        private void ThrowDocumentsOperationContextIsNotSet()
        {
            throw new InvalidOperationException("Documents operation context is not set");
        }

        public virtual string PutDocument(string key, JsValue document, JsValue metadata, JsValue etagJs, Engine engine)
        {
            if (_context == null)
                ThrowDocumentsOperationContextIsNotSet();

            if (document == null || document.IsObject() == false)
            {
                throw new InvalidOperationException(
                    $"Created document must be a valid object which is not null or empty. Document key: '{key}'.");
            }

            long? etag = null;
            if (etagJs != null)
            {
                if (etagJs.IsNumber())
                {
                    etag = (long)etagJs.AsNumber();
                }
                else if (etagJs.IsNull() == false && etagJs.IsUndefined() == false && etagJs.ToString() != "None")
                {
                    throw new InvalidOperationException($"Invalid ETag value for document '{key}'");
                }
            }

            var data = ToBlittable(document.AsObject());
            if (metadata != null && metadata.IsObject())
            {
                data["@metadata"] = ToBlittable(metadata.AsObject());
            }

            var dataReader = _context.ReadObject(data, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            var put = _database.DocumentsStorage.Put(_context, key, etag, dataReader);

            if (DebugMode)
            {
                DebugActions.PutDocument.Add(new DynamicJsonValue
                {
                    ["Key"] = key,
                    ["Etag"] = etag,
                    ["Data"] = dataReader
                });
            }

            return put.Key;
        }

        public virtual void DeleteDocument(string documentKey)
        {
            throw new NotSupportedException("Deleting documents is not supported.");
        }
    }
}