using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Raven.Client;
using Raven.Server.ServerWide.Context;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using System.Linq;

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

        public ObjectInstance ToJsObject(Engine engine, Document document)
        {
            return ToJsObject2(engine, document);
        }

        public static ObjectInstance ToJsObject2(Engine engine, Document document)
        {
            var instance = ToJsObject(engine, document.Data);
            return ApplyMetadataIfNecessary(instance, document.Id, document.ChangeVector, document.LastModified, document.Flags, document.IndexScore);
        }

        public ObjectInstance ToJsObject(Engine engine, DocumentConflict document, string propertyName)
        {
            var instance = ToJsObject(engine, document.Doc);
            return ApplyMetadataIfNecessary(instance, document.Id, document.ChangeVector, document.LastModified, flags: null, indexScore: null);
        }

        private static ObjectInstance ApplyMetadataIfNecessary(ObjectInstance instance, LazyStringValue id, string changeVector, DateTime? lastModified, DocumentFlags? flags, double? indexScore)
        {
            var metadataValue = instance.Get(Constants.Documents.Metadata.Key);
            if (metadataValue == null || metadataValue.IsObject() == false)
                return instance;

            var metadata = metadataValue.AsObject();

            if (changeVector != null)
                metadata.FastAddProperty(Constants.Documents.Metadata.ChangeVector, changeVector, true, true, true);

            if (lastModified.HasValue && lastModified != default(DateTime))
                metadata.FastAddProperty(Constants.Documents.Metadata.LastModified, lastModified.Value.GetDefaultRavenFormat(), true, true, true);

            if (flags.HasValue && flags != DocumentFlags.None)
                metadata.FastAddProperty(Constants.Documents.Metadata.Flags, flags.Value.ToString(), true, true, true);

            if (id != null)
                metadata.FastAddProperty(Constants.Documents.Metadata.Id, id.ToString(), true, true, true);

            if (indexScore.HasValue)
                metadata.FastAddProperty(Constants.Documents.Metadata.IndexScore, indexScore, true, true, true);

            // TOOD: Do we want to expose here also the change vector?

            return instance;
        }

        private static ObjectInstance ToJsObject(Engine engine, BlittableJsonReaderObject json)
        {
            return new BlittableObjectInstance(engine, json);
        }

        private static string CreatePropertyKey(string key, string property)
        {
            if (string.IsNullOrEmpty(property))
                return key;

            return property + "." + key;
        }

        private void ToBlittableJsonReaderObject(ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> writer, ObjectInstance jsObject, string propertyKey = null,
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
            var blittableObjectInstance = jsObject as BlittableObjectInstance;

            if (blittableObjectInstance != null)
            {
                foreach (var propertyIndex in blittableObjectInstance.Blittable.GetPropertiesByInsertionOrder())
                {
                    var prop = new BlittableJsonReaderObject.PropertyDetails();

                    blittableObjectInstance.Blittable.GetPropertyByIndex(propertyIndex, ref prop);

                    writer.WritePropertyName(prop.Name);
                    if (blittableObjectInstance.Modifications != null && blittableObjectInstance.Modifications.TryGetValue(prop.Name, out var modification))
                    {
                        blittableObjectInstance.Modifications.Remove(prop.Name);
                        if (modification.IsDeleted)
                            continue;
                        WriteJsonValue(prop.Name, modification.Value);
                    }
                    else
                    {
                        writer.WriteValue(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                    }
                }

                foreach (var modificationKvp in blittableObjectInstance.Modifications ?? Enumerable.Empty<KeyValuePair<string, (bool isDeleted, JsValue value)>>())
                {
                    if (modificationKvp.Value.isDeleted)
                        continue;

                    writer.WritePropertyName(modificationKvp.Key);
                    WriteJsonValue(modificationKvp.Key, modificationKvp.Value.value);
                }
            }
            else
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

                    writer.WritePropertyName(property.Key);
                    WriteJsonValue(property.Key, value);
                }
            }

            void WriteJsonValue(string name, JsValue value)
            {
                bool recursive = jsObject == value;
                if (recursiveCall && recursive)
                    writer.WriteValueNull();
                else
                {
                    ToBlittableJsonReaderValue(writer, value, CreatePropertyKey(name, propertyKey), recursive);
                }
            }
        }

        internal JsValue ToJsValue(Engine jintEngine, BlittableJsonReaderObject.PropertyDetails propertyDetails)
        {
            switch (propertyDetails.Token & BlittableJsonReaderBase.TypesMask)
            {
                case BlittableJsonToken.Null:
                    return JsValue.Null;
                case BlittableJsonToken.Boolean:
                    return new JsValue((bool)propertyDetails.Value);

                case BlittableJsonToken.Integer:
                    return new JsValue((long)propertyDetails.Value);
                case BlittableJsonToken.LazyNumber:
                    return new JsValue((double)(LazyNumberValue)propertyDetails.Value);
                case BlittableJsonToken.String:
                    return new JsValue(((LazyStringValue)propertyDetails.Value).ToString());
                case BlittableJsonToken.CompressedString:
                    return new JsValue(((LazyCompressedStringValue)propertyDetails.Value).ToString());
                case BlittableJsonToken.StartObject:
                    return new BlittableObjectInstance(jintEngine, (BlittableJsonReaderObject)propertyDetails.Value);
                case BlittableJsonToken.StartArray:
                    //return new BlittableObjectArrayInstance(jintEngine, (BlittableJsonReaderArray)propertyDetails.Value);
                    return BlittableObjectInstance.CreateArrayInstanceBasedOnBlittableArray(jintEngine, propertyDetails.Value as BlittableJsonReaderArray);
                default:
                    throw new ArgumentOutOfRangeException(propertyDetails.Token.ToString());
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool ShouldFilterProperty(string property)
        {
            return property == Constants.Documents.Indexing.Fields.ReduceKeyFieldName ||
                   property == Constants.Documents.Indexing.Fields.DocumentIdFieldName ||
                   property == Constants.Documents.Metadata.Id ||
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

                if (_propertiesByValue.TryGetValue(propertyKey, out KeyValuePair<object, JsValue> property))
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
            // to support static / instance calls. This is ugly, but the code will go away with Jurrasic anyway
            return ToBlittable2(jsObject, propertyKey, recursiveCall);
        }
        public static DynamicJsonValue ToBlittable2(ObjectInstance jsObject, string propertyKey = null, bool recursiveCall = false)
        {
            if (jsObject.Class == "Function")
            {
                // getting a Function instance here,
                // means that we couldn't evaluate it using Jint
                return null;
            }

            var obj = new DynamicJsonValue();

            // todo: maybe treat modifications here?

            var blittableObjectInstance = jsObject as BlittableObjectInstance;

            if (blittableObjectInstance != null)
            {
                foreach (var propertyIndex in blittableObjectInstance.Blittable.GetPropertiesByInsertionOrder())
                {
                    var prop = new BlittableJsonReaderObject.PropertyDetails();

                    blittableObjectInstance.Blittable.GetPropertyByIndex(propertyIndex, ref prop);

                    if (blittableObjectInstance.Modifications != null && blittableObjectInstance.Modifications.TryGetValue(prop.Name, out var modification))
                    {
                        blittableObjectInstance.Modifications.Remove(prop.Name);
                        if (modification.IsDeleted)
                            continue;

                        obj[prop.Name] = ToBlittableValue2(modification.Value, CreatePropertyKey(prop.Name, propertyKey), jsObject == modification.Value, prop.Token, prop.Value);
                    }
                    else
                    {
                        obj[prop.Name] = prop.Value;
                    }
                }

                foreach (var modificationKvp in blittableObjectInstance.Modifications ?? Enumerable.Empty<KeyValuePair<string, (bool isDeleted, JsValue value)>>())
                {
                    var recursive = jsObject == modificationKvp.Value.value;
                    if (recursiveCall && recursive)
                        obj[modificationKvp.Key] = null;
                    else
                        obj[modificationKvp.Key] = ToBlittableValue2(modificationKvp.Value.value, CreatePropertyKey(modificationKvp.Key, propertyKey), jsObject == modificationKvp.Value.value);
                }
            }
            else
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
                    if (recursiveCall && recursive)
                        obj[property.Key] = null;
                    else
                    {
                        var propertyIndexInBlittable = blittableObjectInstance?.Blittable.GetPropertyIndex(property.Key) ?? -1;

                        if (propertyIndexInBlittable < 0)
                        {
                            obj[property.Key] = ToBlittableValue2(value, CreatePropertyKey(property.Key, propertyKey), recursive);
                        }
                        else
                        {
                            var prop = new BlittableJsonReaderObject.PropertyDetails();
                            blittableObjectInstance.Blittable.GetPropertyByIndex(propertyIndexInBlittable, ref prop, true);
                            obj[property.Key] = ToBlittableValue2(value, CreatePropertyKey(property.Key, propertyKey), recursive, prop.Token, prop.Value);
                        }
                    }
                }
            }

            return obj;
        }

        public object ToBlittableValue(JsValue v, string propertyKey, bool recursiveCall, BlittableJsonToken? token = null, object originalValue = null)
        {
            // ugly and temporary
            return ToBlittableValue2(v, propertyKey, recursiveCall, token, originalValue);
        }
        public static object ToBlittableValue2(JsValue v, string propertyKey, bool recursiveCall, BlittableJsonToken? token = null, object originalValue = null)
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

                if (originalValue != null && token.HasValue && (
                    (token.Value & BlittableJsonToken.LazyNumber) == BlittableJsonToken.LazyNumber ||
                    (token.Value & BlittableJsonToken.Integer) == BlittableJsonToken.Integer))
                {
                    // If the current value is exactly as the original value, we can return the original value before we made the JS conversion, 
                    // which will convert a Int64 to jsFloat.

                    double originalDouble;
                    if (originalValue is LazyNumberValue ldv)
                        originalDouble = ldv;
                    else
                        originalDouble = Convert.ToDouble(originalValue);

                    if (Math.Abs(num - originalDouble) < double.Epsilon)
                        return originalValue;

                    //We might have change the type of num from Integer to long in the script by design 
                    //Making sure the number isn't a real float before returning it as integer
                    if (originalValue is int && (Math.Abs(num - Math.Floor(num)) <= double.Epsilon || Math.Abs(num - Math.Ceiling(num)) <= double.Epsilon))
                        return (long)num;
                    return num; //float
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

                    var ravenJToken = ToBlittableValue2(jsInstance, propertyKey + "[" + property.Key + "]", recursiveCall);

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
                return ToBlittable2(v.AsObject(), propertyKey, recursiveCall);
            }
            if (v.IsRegExp())
                return null;

            throw new NotSupportedException(v.Type.ToString());
        }

        public void Dispose()
        {
        }

        public virtual JsValue LoadDocument(string documentId, Engine engine, ref int totalStatements)
        {
            if (_context == null)
                ThrowDocumentsOperationContextIsNotSet();

            var document = _database.DocumentsStorage.Get(_context, documentId);

            if (DebugMode)
                DebugActions.LoadDocument.Add(documentId);

            if (document == null)
                return JsValue.Null;

            totalStatements += (MaxSteps / 2 + (document.Data.Size * AdditionalStepsPerSize));
            engine.Options.MaxStatements(totalStatements);

            // TODO: Make sure to add Constants.Indexing.Fields.DocumentIdFieldName to document.Data
            return ToJsObject(engine, document.Data);
        }

        private static void ThrowDocumentsOperationContextIsNotSet()
        {
            throw new InvalidOperationException("Documents operation context is not set");
        }

        public virtual string PutDocument(string id, JsValue document, JsValue metadata, string changeVector, Engine engine)
        {
            if (_context == null)
                ThrowDocumentsOperationContextIsNotSet();

            if (document == null || document.IsObject() == false)
            {
                throw new InvalidOperationException(
                    $"Created document must be a valid object which is not null or empty. Document ID: '{id}'.");
            }

            var data = ToBlittable(document.AsObject());
            if (metadata != null && metadata.IsObject())
            {
                data["@metadata"] = ToBlittable(metadata.AsObject());
            }
            var dataReader = _context.ReadObject(data, id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            var put = _database.DocumentsStorage.Put(_context, id, _context.GetLazyString(changeVector), dataReader);

            if (DebugMode)
            {
                DebugActions.PutDocument.Add(new DynamicJsonValue
                {
                    ["Id"] = id,
                    ["ChangeVector"] = changeVector,
                    ["Data"] = dataReader
                });
            }

            return put.Id;
        }

        public virtual void DeleteDocument(string documentId)
        {
            throw new NotSupportedException("Deleting documents is not supported.");
        }
    }
}
