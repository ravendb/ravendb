using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using Raven.Client;
using Raven.Server.ServerWide.Context;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using System.Linq;
using Jurassic.Library;
using Jurassic;

namespace Raven.Server.Documents.Patch
{
    public class PatcherOperationScope : IDisposable
    {
        private readonly DocumentDatabase _database;

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

        public object ActualPatchResult { get; set; }
        public int MaxSteps;

        public ObjectInstance PatchObject;

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

        public ObjectInstance ToJsObject(ScriptEngine engine, Document document)
        {
            return ToJsObject2(engine, document);
        }

        public static ObjectInstance ToJsObject2(ScriptEngine engine, Document document)
        {
            var instance = ToJsObject(engine, document.Data);
            return ApplyMetadataIfNecessary(instance, document.Id, document.ChangeVector, document.LastModified, document.Flags, document.IndexScore);
        }

        public ObjectInstance ToJsObject(ScriptEngine engine, DocumentConflict document, string propertyName)
        {
            var instance = ToJsObject(engine, document.Doc);
            return ApplyMetadataIfNecessary(instance, document.Id, document.ChangeVector, document.LastModified, flags: null, indexScore: null);
        }

        private static ObjectInstance ApplyMetadataIfNecessary(ObjectInstance instance, LazyStringValue id, string changeVector, DateTime? lastModified, DocumentFlags? flags, double? indexScore)
        {
            var metadataValue = instance.GetPropertyValue(Constants.Documents.Metadata.Key);

            if (metadataValue == null || metadataValue is ArrayInstance)
                return instance;

            var metadata = metadataValue as ObjectInstance;

            if (changeVector != null)
                metadata.SetPropertyValue(Constants.Documents.Metadata.ChangeVector, changeVector, true);

            if (lastModified.HasValue && lastModified != default(DateTime))
                metadata.SetPropertyValue(Constants.Documents.Metadata.LastModified, lastModified.Value.GetDefaultRavenFormat(), true);

            if (flags.HasValue && flags != DocumentFlags.None)
                metadata.SetPropertyValue(Constants.Documents.Metadata.Flags, flags.Value.ToString(), true);

            if (id != null)
                metadata.SetPropertyValue(Constants.Documents.Metadata.Id, id.ToString(), true);

            if (indexScore.HasValue)
                metadata.SetPropertyValue(Constants.Documents.Metadata.IndexScore, indexScore, true);

            return instance;
        }

        private static ObjectInstance ToJsObject(ScriptEngine engine, BlittableJsonReaderObject json)
        {
            return new BlittableObjectInstance(engine, json, null);
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
                var properties = blittableObjectInstance.Properties.ToDictionary(x => x.Key.ToString(), x => x.Value);
                foreach (var propertyIndex in blittableObjectInstance.Blittable.GetPropertiesByInsertionOrder())
                {
                    var prop = new BlittableJsonReaderObject.PropertyDetails();

                    blittableObjectInstance.Blittable.GetPropertyByIndex(propertyIndex, ref prop);

                    if (blittableObjectInstance.Deletes.Contains(prop.Name))
                        continue;

                    writer.WritePropertyName(prop.Name);

                    if (properties.Remove(prop.Name, out var modifiedValue))
                    {
                        WriteJsonValue(prop.Name, modifiedValue);
                    }
                    else
                    {
                        writer.WriteValue(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                    }
                }

                foreach (var modificationKvp in properties)
                {
                    writer.WritePropertyName(modificationKvp.Key);
                    WriteJsonValue(modificationKvp.Key, modificationKvp.Value);
                }
            }
            else
            {
                var properties = jsObject.Properties.ToList();
                foreach (var property in properties)
                {
                    var propertyName = property.Key.ToString();
                    if (ShouldFilterProperty(propertyName))
                        continue;

                    var value = property.Value;
                    if (value == null)
                        continue;

                    if (value is RegExpInstance)
                        continue;

                    writer.WritePropertyName(propertyName);
                    WriteJsonValue(propertyName, value);
                }
            }

            void WriteJsonValue(string name, object value)
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

        internal object ToJsValue(ScriptEngine jintEngine, BlittableJsonReaderObject.PropertyDetails propertyDetails)
        {
            switch (propertyDetails.Token & BlittableJsonReaderBase.TypesMask)
            {
                case BlittableJsonToken.Null:
                    return Null.Value;
                case BlittableJsonToken.Boolean:
                    return (bool)propertyDetails.Value;
                case BlittableJsonToken.Integer:
                    return (long)propertyDetails.Value;
                case BlittableJsonToken.LazyNumber:
                    return (double)(LazyNumberValue)propertyDetails.Value;
                case BlittableJsonToken.String:
                    return ((LazyStringValue)propertyDetails.Value).ToString();
                case BlittableJsonToken.CompressedString:
                    return ((LazyCompressedStringValue)propertyDetails.Value).ToString();
                case BlittableJsonToken.StartObject:
                    return new BlittableObjectInstance(jintEngine, (BlittableJsonReaderObject)propertyDetails.Value, null);
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

        private void ToBlittableJsonReaderValue(ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> writer, object v, string propertyKey, bool recursiveCall)
        {
            var vType = v.GetType();
            var typeCode = Type.GetTypeCode(vType);

            switch (typeCode)
            {
                case TypeCode.Boolean:
                    writer.WriteValue((bool)v);
                    return;
                case TypeCode.String:
                    var value = v.ToString();
                    writer.WriteValue(value);
                    return;
                case TypeCode.Byte:
                    writer.WriteValue((byte)v);
                    return;
                case TypeCode.SByte:
                    writer.WriteValue((SByte)v);
                    return;
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.Int16:
                case TypeCode.Int32:
                    writer.WriteValue((int)v);
                    return;
                case TypeCode.UInt64:
                case TypeCode.Int64:
                    writer.WriteValue((long)v);
                    break;
                case TypeCode.Decimal:
                    writer.WriteValue((decimal)v);
                    return;
                case TypeCode.Double:
                case TypeCode.Single:
                    writer.WriteValue((double)v);
                    return;
                case TypeCode.DateTime:
                    writer.WriteValue(((DateTime)v).ToString(Default.DateTimeFormatsToWrite));
                    return;
            }


            if (v == Null.Value || v == Undefined.Value)
            {
                writer.WriteValueNull();
                return;
            }
            if (v is ArrayInstance)
            {
                var jsArray = v as ArrayInstance;
                writer.StartWriteArray();
                foreach (var property in jsArray.Properties)
                {
                    if (InheritedProperties.Contains(property.Key))
                        continue;

                    var jsInstance = property.Value;
                    if (jsInstance == null)
                        continue;

                    ToBlittableJsonReaderValue(writer, jsInstance, propertyKey + "[" + property.Key + "]",
                        recursiveCall);
                }
                writer.WriteArrayEnd();
                return;
            }
            if (v is RegExpInstance)
            {
                writer.WriteValueNull();
                return;
            }
            if (v is ObjectInstance)
            {
                ToBlittableJsonReaderObject(writer, v as ObjectInstance, propertyKey, recursiveCall);
                return;
            }

            throw new NotSupportedException(v.GetType().ToString());
        }

        public DynamicJsonValue ToBlittable(ObjectInstance jsObject, string propertyKey = null, bool recursiveCall = false)
        {
            // to support static / instance calls. This is ugly, but the code will go away with Jurrasic anyway
            return ToBlittable2(jsObject, propertyKey, recursiveCall);
        }

        public static DynamicJsonValue ToBlittable2(ObjectInstance jsObject, string propertyKey = null, bool recursiveCall = false)
        {
            if (jsObject is FunctionInstance)
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
                var properties = blittableObjectInstance.Properties.ToDictionary(x => x.Key.ToString(), x => x.Value);

                foreach (var propertyIndex in blittableObjectInstance.Blittable.GetPropertiesByInsertionOrder())
                {
                    var prop = new BlittableJsonReaderObject.PropertyDetails();

                    blittableObjectInstance.Blittable.GetPropertyByIndex(propertyIndex, ref prop);

                    if (blittableObjectInstance.Deletes.Contains(prop.Name))
                        continue;

                    if (properties.Remove(prop.Name, out var modifiedValue))
                    {
                        obj[prop.Name] = ToBlittableValue2(modifiedValue, CreatePropertyKey(prop.Name, propertyKey), jsObject == modifiedValue, prop.Token, prop.Value);
                    }
                    else
                    {
                        obj[prop.Name] = prop.Value;
                    }
                }

                foreach (var modificationKvp in properties)
                {
                    var recursive = jsObject == modificationKvp.Value;
                    if (recursiveCall && recursive)
                        obj[modificationKvp.Key] = null;
                    else
                        obj[modificationKvp.Key] = ToBlittableValue2(modificationKvp.Value, CreatePropertyKey(modificationKvp.Key, propertyKey), jsObject == modificationKvp.Value);
                }
            }
            else
            {
                var properties = jsObject.Properties.ToList();
                foreach (var property in properties)
                {
                    var propertyName = property.Key.ToString();
                    if (ShouldFilterProperty(propertyName))
                        continue;

                    var value = property.Value;
                    if (value == null)
                        continue;

                    if (value is RegExpInstance)
                        continue;

                    var recursive = jsObject == value;
                    if (recursiveCall && recursive)
                        obj[propertyName] = null;
                    else
                    {
                        var propertyIndexInBlittable = blittableObjectInstance?.Blittable.GetPropertyIndex(propertyName) ?? -1;

                        if (propertyIndexInBlittable < 0)
                        {
                            obj[propertyName] = ToBlittableValue2(value, CreatePropertyKey(propertyName, propertyKey), recursive);
                        }
                        else
                        {
                            var prop = new BlittableJsonReaderObject.PropertyDetails();
                            blittableObjectInstance.Blittable.GetPropertyByIndex(propertyIndexInBlittable, ref prop, true);
                            obj[propertyName] = ToBlittableValue2(value, CreatePropertyKey(propertyName, propertyKey), recursive, prop.Token, prop.Value);
                        }
                    }
                }
            }

            return obj;
        }

        public object ToBlittableValue(object v, string propertyKey, bool recursiveCall, BlittableJsonToken? token = null, object originalValue = null)
        {
            // ugly and temporary
            return ToBlittableValue2(v, propertyKey, recursiveCall, token, originalValue);
        }

        public static object ToBlittableValue2(object v, string propertyKey, bool recursiveCall, BlittableJsonToken? token = null, object originalValue = null)
        {
            var vType = v.GetType();
            var typeCode = Type.GetTypeCode(vType);

            switch (typeCode)
            {
                case TypeCode.Boolean:
                    return (bool)v;
                case TypeCode.String:
                    const string RavenDataByteArrayToBase64 = "raven-data:byte[];base64,";
                    var value = v.ToString();
                    if (value != null && value.StartsWith(RavenDataByteArrayToBase64))
                    {
                        value = value.Remove(0, RavenDataByteArrayToBase64.Length);
                        var byteArray = Convert.FromBase64String(value);
                        return Encoding.UTF8.GetString(byteArray);
                    }
                    return value;
                case TypeCode.Byte:
                case TypeCode.SByte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.UInt64:
                case TypeCode.Int64:
                case TypeCode.Decimal:
                case TypeCode.Double:
                case TypeCode.Single:
                    return v;
                case TypeCode.DateTime:
                    return ((DateTime)v).ToString(Default.DateTimeFormatsToWrite);
            }

            if (v == Null.Value || v == Undefined.Value)
            {
                return null;
            }
            if (v is ArrayInstance)
            {
                var jsArray = v as ArrayInstance;
                var array = new DynamicJsonArray();

                foreach (var property in jsArray.Properties)
                {
                    if (InheritedProperties.Contains(property.Key))
                        continue;

                    var jsInstance = property.Value;
                    if (jsInstance == null)
                        continue;

                    var ravenJToken = ToBlittableValue2(jsInstance, propertyKey + "[" + property.Key + "]", recursiveCall);

                    if (ravenJToken == null)
                        continue;

                    array.Add(ravenJToken);
                }

                return array;
            }

            if (v is RegExpInstance)
            {
                return null;
            }

            if (v is ObjectInstance)
            {
                return ToBlittable2(v as ObjectInstance, propertyKey, recursiveCall);
            }

            throw new NotSupportedException(v.GetType().ToString());
        }

        public void Dispose()
        {
        }

        public virtual object LoadDocument(string documentId, ScriptEngine engine)
        {
            if (_context == null)
                ThrowDocumentsOperationContextIsNotSet();

            var document = _database.DocumentsStorage.Get(_context, documentId);

            if (DebugMode)
                DebugActions.LoadDocument.Add(documentId);

            if (document == null)
                return Null.Value;

            return ToJsObject(engine, document.Data);
        }

        private static void ThrowDocumentsOperationContextIsNotSet()
        {
            throw new InvalidOperationException("Documents operation context is not set");
        }

        public virtual string PutDocument(string id, object document, object metadata, string changeVector, ScriptEngine engine)
        {
            if (_context == null)
                ThrowDocumentsOperationContextIsNotSet();

            if (document == null || document is ObjectInstance == false)
            {
                throw new InvalidOperationException(
                    $"Created document must be a valid object which is not null or empty. Document ID: '{id}'.");
            }

            var data = ToBlittable(document as ObjectInstance);
            if (metadata != null && metadata is ObjectInstance)
            {
                data["@metadata"] = ToBlittable(metadata as ObjectInstance);
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
