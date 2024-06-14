using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using Corax;
using Corax.Mappings;
using Jint;
using Jint.Native;
using Jint.Native.Array;
using Jint.Native.Object;
using Jint.Runtime;
using Jint.Runtime.Descriptors;
using Jint.Runtime.Interop;
using Lucene.Net.Store;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static.JavaScript;
using Raven.Server.Documents.Queries.Results;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Documents.Patch
{
    [DebuggerDisplay("Blittable JS object")]
    public sealed class BlittableObjectInstance : ObjectInstance
    {
        public bool Changed;
        private readonly BlittableObjectInstance _parent;
        private readonly Document _doc;
        private bool _set;

        public readonly DateTime? LastModified;
        public readonly string ChangeVector;
        public readonly BlittableJsonReaderObject Blittable;
        public readonly string DocumentId;
        public HashSet<string> Deletes;
        public Dictionary<string, BlittableObjectProperty> OwnValues;
        public Dictionary<string, BlittableJsonToken> OriginalPropertiesTypes;
        public RetrieverInput IndexRetriever;
        public Dictionary<string, IndexField> IndexFields;
        public bool AnyDynamicIndexFields;

        public ProjectionOptions Projection;

        public SpatialResult? Distance => _doc?.Distance;
        public float? IndexScore => _doc?.IndexScore;

        private void MarkChanged()
        {
            Changed = true;
            _parent?.MarkChanged();
        }

        public ObjectInstance GetOrCreate(string key)
        {
            BlittableObjectProperty property = default;
            if (OwnValues?.TryGetValue(key, out property) == true &&
                property != null)
                return property.Value.AsObject();

            property = GenerateProperty(key);

            OwnValues ??= new Dictionary<string, BlittableObjectProperty>(Blittable.Count);

            OwnValues[key] = property;
            Deletes?.Remove(key);

            return property.Value.AsObject();

            BlittableObjectProperty GenerateProperty(string propertyName)
            {
                var propertyIndex = Blittable.GetPropertyIndex(propertyName);

                var prop = new BlittableObjectProperty(this, propertyName);
                if (propertyIndex == -1)
                {
                    prop.Value = new ObjectInstance(Engine);
                }

                return prop;
            }
        }

        public sealed class BlittableObjectProperty : PropertyDescriptor
        {
            private readonly BlittableObjectInstance _parent;
            private readonly string _property;
            private JsValue _value;
            public bool Changed;

            public override string ToString()
            {
                return _property;
            }

            public BlittableObjectProperty(BlittableObjectInstance parent, string property)
                : base(PropertyFlag.CustomJsValue | PropertyFlag.Writable | PropertyFlag.WritableSet | PropertyFlag.Enumerable | PropertyFlag.EnumerableSet)
            {
                _parent = parent;
                _property = property;

                if (TryGetValueFromIndex(_parent, _property, out _value) == false)
                {
                    if (_parent.Projection?.MustExtractFromIndex == true)
                    {
                        if (_parent.Projection.MustExtractOrThrow)
                            _parent.Projection.ThrowCouldNotExtractFieldFromIndexBecauseIndexDoesNotContainSuchFieldOrFieldValueIsNotStored(property);

                        _value = JsValue.Undefined;
                        return;
                    }

                    if (TryGetValueFromDocument(_parent, _property, out _value) == false)
                    {
                        if (_parent.Projection?.MustExtractFromDocument == true)
                        {
                            if (_parent.Projection.MustExtractOrThrow)
                                _parent.Projection.ThrowCouldNotExtractFieldFromDocumentBecauseDocumentDoesNotContainSuchField(_parent.DocumentId, property);
                        }

                        _value = JsValue.Undefined;
                    }
                }
            }
            
            private bool TryGetValueFromIndex(BlittableObjectInstance parent, string property, out JsValue value)
            {
                value = null;
                if (parent.Projection?.MustExtractFromDocument == true)
                    return false;

                if (parent.IndexFields == null)
                    return false;

                if (parent.IndexFields.TryGetValue(_property, out var indexField) == false && parent.AnyDynamicIndexFields == false)
                    return false;

                bool isLucene = parent.IndexRetriever.IsLuceneDocument();

                return isLucene
                    ? TryGetValueFromLucene(parent, property, indexField, out value)
                    : TryGetValueFromCorax(parent, property, indexField, out value);
            }
            
            private unsafe bool TryGetValueFromCorax(BlittableObjectInstance parent, string property, IndexField indexField, out JsValue value)
            {
                value = null;
                var fieldMapping = parent.IndexRetriever.KnownFields;
                var isDynamic = indexField == null;
                IndexFieldBinding binding = null;
                if (isDynamic == false && fieldMapping.TryGetByFieldId(indexField.Id, out binding) == false)
                    return false;

                ref var reader = ref parent.IndexRetriever.CoraxEntry;
                var fieldReader = binding is not null
                    ? reader.GetFieldReaderFor(binding.Metadata)
                    : reader.GetFieldReaderFor(parent.IndexRetriever.CoraxIndexSearcher.GetDynamicFieldName(property));

                switch (fieldReader.Type)
                {
                    case IndexEntryFieldType.Empty:
                        value = string.Empty;
                        break;
                    case IndexEntryFieldType.Null:
                        value = DynamicJsNull.ExplicitNull;
                        break;
                    case IndexEntryFieldType.TupleListWithNulls:
                    case IndexEntryFieldType.TupleList:
                    {
                        if (fieldReader.TryReadMany(out var iterator) == false)
                            goto case IndexEntryFieldType.Invalid;

                        var tupleList = new string[iterator.Count];

                        for (int idX = 0; idX < iterator.Count && iterator.ReadNext(); ++idX)
                        {
                            if (iterator.IsNull)
                                tupleList[idX] = null;
                            else if (iterator.IsEmptyCollection)
                            {
                                var identifierFound = reader.GetFieldReaderFor(0).Read(out var identifier);
                                throw new InvalidDataException($"Tuple list cannot contain an empty string (otherwise, where did the numeric came from!)" +
                                                               $"{Environment.NewLine}Field: {binding?.FieldNameAsString ?? property}" +
                                                               $"{Environment.NewLine}Document: {(identifierFound ? Encodings.Utf8.GetString(identifier) : "(unknown)")}");
                            }
                            else
                                tupleList[idX] = Encodings.Utf8.GetString(iterator.Sequence);
                        }
                        
                        value = FromObject(parent.Engine, tupleList);
                    }
                        break;

                    case IndexEntryFieldType.Tuple:
                    {
                        if (fieldReader.TryReadTuple(out long lVal, out double dVal, out Span<byte> valueInEntry) == false)
                            goto case IndexEntryFieldType.Invalid;
                        var hasTime = parent.IndexRetriever.IndexFieldsPersistence.HasTimeValues(binding?.FieldNameAsString ?? property);
                        if (hasTime)
                            value = Encodings.Utf8.GetString(valueInEntry);
                        else if (dVal.AlmostEquals(lVal))
                            value = lVal;
                        else
                            value = dVal;
                    }

                        break;

                    case IndexEntryFieldType.SpatialPointList:
                    {
                        if (fieldReader.TryReadManySpatialPoint(out var spatialIterator) == false)
                            goto case IndexEntryFieldType.Invalid;

                        var geoList = new string[spatialIterator.Count];
                        for (int idX = 0; idX < spatialIterator.Count && spatialIterator.ReadNext(); ++idX)
                            geoList[idX] = Encodings.Utf8.GetString(spatialIterator.Geohash);
                        
                        value = FromObject(parent.Engine, geoList);
                    }

                        break;

                    case IndexEntryFieldType.SpatialPoint:
                    {
                        if (fieldReader.Read(out var valueInEntry) == false)
                            goto case IndexEntryFieldType.Invalid;

                        value = Encodings.Utf8.GetString(valueInEntry);
                    } 
                        break;
                    case IndexEntryFieldType.ListWithEmpty:
                    case IndexEntryFieldType.ListWithNulls:
                    case IndexEntryFieldType.List:
                    {
                        if (fieldReader.TryReadMany(out var iterator) == false)
                            goto case IndexEntryFieldType.Invalid;
                        
                        var array = new string[iterator.Count];
                        for (int idX = 0; idX < iterator.Count && iterator.ReadNext(); ++idX)
                        {
                            if (iterator.IsEmptyString || iterator.IsNull)
                                array[idX] = (iterator.IsNull ? null : string.Empty);
                            else
                                array[idX] = Encodings.Utf8.GetString(iterator.Sequence);
                        }
                        
                        value = FromObject(parent.Engine, array);
                    }
                        break;
                    case IndexEntryFieldType.RawList:
                    {
                        if (fieldReader.TryReadMany(out var iterator) == false)
                            goto case IndexEntryFieldType.Invalid;

                        JsValue[] arrayItems = new JsValue[iterator.Count];

                        for (int idX = 0; idX < iterator.Count && iterator.ReadNext(); ++idX)
                        {
                            fixed (byte* ptr = &iterator.Sequence.GetPinnableReference())
                            {
                                var itemAsBlittable = new BlittableJsonReaderObject(ptr, iterator.Sequence.Length, parent.Blittable._context);
                                arrayItems[idX] = TranslateToJs(parent, indexField?.Name ?? property, BlittableJsonToken.StartObject, itemAsBlittable);
                            }
                        }
                        
                        value = FromObject(parent.Engine, arrayItems);
                    }
                        break;
                    case IndexEntryFieldType.Raw:
                    {
                        if (fieldReader.Read(out Span<byte> blittableBinary) == false)
                            goto case IndexEntryFieldType.Invalid;

                        fixed (byte* ptr = &blittableBinary.GetPinnableReference())
                        {
                            var itemAsBlittable = new BlittableJsonReaderObject(ptr, blittableBinary.Length, parent.Blittable._context);
                            value = TranslateToJs(parent, indexField?.Name ?? property, BlittableJsonToken.StartObject, itemAsBlittable);
                        }
                    }
                        break;
                    case IndexEntryFieldType.Invalid:
                        value = null;
                        return false;
                    default:
                    {
                        if (fieldReader.Read(out var valueInEntry) == false)
                            goto case IndexEntryFieldType.Invalid;

                        value = Encodings.Utf8.GetString(valueInEntry);
                    }
                        break;
                }
                return true;
            }

            private bool TryGetValueFromDocument(BlittableObjectInstance parent, string property, out JsValue value)
            {
                value = null;

                var index = parent.Blittable?.GetPropertyIndex(property);
                if (index == null || index == -1)
                    return false;

                var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();

                parent.Blittable.GetPropertyByIndex(index.Value, ref propertyDetails, true);

                value = TranslateToJs(parent, property, propertyDetails.Token, propertyDetails.Value);
                return true;
            }

            private bool TryGetValueFromLucene(BlittableObjectInstance parent, string property, IndexField indexField, out JsValue value)
            {
                value = null;
                
                if (indexField != null && indexField.Storage == FieldStorage.No)
                    return false;

                var fieldType = QueryResultRetrieverBase.GetFieldType(property, parent.IndexRetriever.LuceneDocument);
                if (fieldType.IsArray)
                {
                    // here we need to perform a manipulation in order to generate the object from the data
                    if (fieldType.IsJson)
                    {
                        Lucene.Net.Documents.Field[] propertyFields = parent.IndexRetriever.LuceneDocument.GetFields(property);

                        JsValue[] arrayItems =
                            new JsValue[propertyFields.Length];

                        for (int i = 0; i < propertyFields.Length; i++)
                        {
                            var field = propertyFields[i];
                            var stringValue = field.StringValue(parent.IndexRetriever.State);

                            var itemAsBlittable = parent.Blittable._context.Sync.ReadForMemory(stringValue, field.Name);

                            arrayItems[i] = TranslateToJs(parent, field.Name, BlittableJsonToken.StartObject, itemAsBlittable);
                        }

                        value = FromObject(parent.Engine, arrayItems);
                        return true;
                    }

                    var values = parent.IndexRetriever.LuceneDocument.GetValues(property, parent.IndexRetriever.State);
                    value = FromObject(parent.Engine, values);
                    return true;
                }

                var fieldable = _parent.IndexRetriever.LuceneDocument.GetFieldable(property);
                if (fieldable == null)
                    return false;

                var val = fieldable.StringValue(_parent.IndexRetriever.State);
                if (fieldType.IsJson)
                {
                    BlittableJsonReaderObject valueAsBlittable = parent.Blittable._context.Sync.ReadForMemory(val, property);
                    value = TranslateToJs(parent, property, BlittableJsonToken.StartObject, valueAsBlittable);
                    return true;
                }

                if (fieldable.IsTokenized == false)
                {
                    // NULL_VALUE and EMPTY_STRING fields aren't tokenized
                    // this will prevent converting fields with a "NULL_VALUE" string to null
                    switch (val)
                    {
                        case Client.Constants.Documents.Indexing.Fields.NullValue:
                            value = DynamicJsNull.ExplicitNull;
                            return true;

                        case Client.Constants.Documents.Indexing.Fields.EmptyString:
                            value = string.Empty;
                            return true;
                    }
                }

                if (fieldType.IsNumeric)
                {
                    if (long.TryParse(val, NumberStyles.Integer, CultureInfo.InvariantCulture, out var valueAsLong))
                    {
                        value = valueAsLong;
                    }
                    else if (double.TryParse(val, NumberStyles.Any, CultureInfo.InvariantCulture, out var valueAsDouble))
                    {
                        value = valueAsDouble;
                    }
                    else
                    {
                        throw new InvalidOperationException($"Recognized field '{property}' as numeric but was unable to parse its value to 'long' or 'double'. " +
                                                            $"documentId = '{parent.DocumentId}', value = {val}.");
                    }
                }
                else
                {
                    value = val;
                }

                return true;
            }

            protected override JsValue CustomValue
            {
                get => _value;
                set
                {
                    if (Equals(value, _value))
                        return;
                    _value = value;
                    _parent.MarkChanged();
                    Changed = true;
                }
            }

            private ArrayInstance GetArrayInstanceFromBlittableArray(Engine e, BlittableJsonReaderArray bjra, BlittableObjectInstance parent)
            {
                bjra.NoCache = true;

                PropertyDescriptor[] items = new PropertyDescriptor[bjra.Length];
                for (var i = 0; i < bjra.Length; i++)
                {
                    var json = bjra.GetValueTokenTupleByIndex(i);
                    BlittableJsonToken itemType = json.Item2 & BlittableJsonReaderBase.TypesMask;
                    JsValue item;
                    if (itemType == BlittableJsonToken.Integer || itemType == BlittableJsonToken.LazyNumber)
                    {
                        item = TranslateToJs(null, null, json.Item2, json.Item1);
                    }
                    else
                    {
                        item = TranslateToJs(parent, null, json.Item2, json.Item1);
                    }
                    items[i] = new PropertyDescriptor(item, true, true, true);
                }

                var jsArray = new ArrayInstance(e, items);
                jsArray.SetPrototypeOf(e.Array.PrototypeObject);

                return jsArray;
            }

            private JsValue TranslateToJs(BlittableObjectInstance owner, string key, BlittableJsonToken type, object value)
            {
                switch (type & BlittableJsonReaderBase.TypesMask)
                {
                    case BlittableJsonToken.Null:
                        return DynamicJsNull.ExplicitNull;

                    case BlittableJsonToken.Boolean:
                        return (bool)value ? JsBoolean.True : JsBoolean.False;

                    case BlittableJsonToken.Integer:
                        // TODO: in the future, add [numeric type]TryFormat, when parsing numbers to strings
                        owner?.RecordNumericFieldType(key, BlittableJsonToken.Integer);
                        return (long)value;

                    case BlittableJsonToken.LazyNumber:
                        owner?.RecordNumericFieldType(key, BlittableJsonToken.LazyNumber);
                        return GetJsValueForLazyNumber(owner?.Engine, (LazyNumberValue)value);

                    case BlittableJsonToken.String:
                        return value.ToString();

                    case BlittableJsonToken.CompressedString:
                        return value.ToString();

                    case BlittableJsonToken.StartObject:
                        Changed = true;
                        _parent.MarkChanged();
                        BlittableJsonReaderObject blittable = (BlittableJsonReaderObject)value;

                        var obj = Raven.Server.Utils.TypeConverter.TryConvertBlittableJsonReaderObject(blittable);
                        switch (obj)
                        {
                            case BlittableJsonReaderArray blittableArray:
                                return GetArrayInstanceFromBlittableArray(owner.Engine, blittableArray, owner);

                            case LazyStringValue asLazyStringValue:
                                return asLazyStringValue.ToString();

                            case LazyCompressedStringValue asLazyCompressedStringValue:
                                return asLazyCompressedStringValue.ToString();

                            default:
                                blittable.NoCache = true;
                                return new BlittableObjectInstance(owner.Engine,
                                    owner,
                                    blittable, null, null, null);
                        }

                    case BlittableJsonToken.StartArray:
                        Changed = true;
                        _parent.MarkChanged();
                        var array = (BlittableJsonReaderArray)value;
                        return GetArrayInstanceFromBlittableArray(owner.Engine, array, owner);

                    default:
                        throw new ArgumentOutOfRangeException(type.ToString());
                }
            }

            public static JsValue GetJsValueForLazyNumber(Engine engine, LazyNumberValue value)
            {
                // First, try and see if the number is withing double boundaries.
                // We use double's tryParse and it actually may round the number,
                // But that are Jint's limitations
                if (value.TryParseDouble(out double doubleVal))
                {
                    return doubleVal;
                }

                // If number is not in double boundaries, we return the LazyNumberValue
                return new ObjectWrapper(engine, value);
            }
        }

        public BlittableObjectInstance(Engine engine,
            BlittableObjectInstance parent,
            BlittableJsonReaderObject blittable,
            string id,
            DateTime? lastModified,
            string changeVector) : base(engine)
        {
            _parent = parent;
            blittable.NoCache = true;
            LastModified = lastModified;
            ChangeVector = changeVector;
            Blittable = blittable;
            DocumentId = id;

            SetPrototypeOf(engine.Object.PrototypeObject);
        }

        public BlittableObjectInstance(Engine engine,
            BlittableObjectInstance parent,
            BlittableJsonReaderObject blittable,
            Document doc) : this(engine, parent, blittable, doc.Id, doc.LastModified, doc.ChangeVector)
        {
            _doc = doc;
        }

        public bool TryGetOriginalDocumentIfUnchanged(out Document doc)
        {
            doc = _doc;
            return _doc != null && Changed == false;
        }

        public override bool Delete(JsValue property)
        {
            if (property.IsString() == false)
                return false;

            string name = property.AsString();

            if (Deletes == null)
                Deletes = new HashSet<string>();

            var desc = GetOwnProperty(name);

            if (desc == PropertyDescriptor.Undefined)
                return true;

            MarkChanged();
            Deletes.Add(name);
            return OwnValues?.Remove(name) == true;
        }

        public override PropertyDescriptor GetOwnProperty(JsValue property)
        {
            if (property.IsString() == false)
                return PropertyDescriptor.Undefined;

            return GetOwnProperty(property.AsString());
        }

        public PropertyDescriptor GetOwnProperty(string property)
        {
            BlittableObjectProperty val = default;
            if (OwnValues?.TryGetValue(property, out val) == true &&
                val != null)
                return val;

            Deletes?.Remove(property);

            val = new BlittableObjectProperty(this, property);

            if (val.Value.IsUndefined() &&
                DocumentId == null &&
                _set == false)
            {
                return PropertyDescriptor.Undefined;
            }

            OwnValues ??= new Dictionary<string, BlittableObjectProperty>(Blittable.Count);

            OwnValues[property] = val;

            return val;
        }

        public override bool Set(JsValue property, JsValue value, JsValue receiver)
        {
            if (property.IsString() == false)
                return false;

            // check fast path for direct write
            if (ReferenceEquals(receiver, this) && Extensible)
            {
                return SetDirect(property.AsString(), value);
            }

            _set = true;
            try
            {
                return base.Set(property, value, receiver);
            }
            finally
            {
                _set = false;
            }
        }

        public override bool DefineOwnProperty(JsValue property, PropertyDescriptor desc)
        {
            if (property.IsString() == false)
                return false;

            return SetDirect(property.AsString(), desc.Value);
        }

        private bool SetDirect(string property, JsValue value)
        {
            OwnValues ??= new Dictionary<string, BlittableObjectProperty>(Blittable.Count);
            Deletes?.Remove(property);

            var val = new BlittableObjectProperty(this, property)
            {
                Value = value
            };

            OwnValues[property] = val;

            return true;
        }

        public override IEnumerable<KeyValuePair<JsValue, PropertyDescriptor>> GetOwnProperties()
        {
            if (OwnValues != null)
            {
                foreach (var value in OwnValues)
                {
                    yield return new KeyValuePair<JsValue, PropertyDescriptor>(value.Key, value.Value);
                }
            }

            if (Blittable == null)
                yield break;

            foreach (var prop in Blittable.GetPropertyNames())
            {
                if (Deletes?.Contains(prop) == true)
                    continue;
                if (OwnValues?.ContainsKey(prop) == true)
                    continue;
                yield return new KeyValuePair<JsValue, PropertyDescriptor>(
                    prop,
                    GetOwnProperty(prop)
                    );
            }
        }

        public override List<JsValue> GetOwnPropertyKeys(Types types)
        {
            var list = new List<JsValue>(Blittable?.Count ?? OwnValues?.Count ?? 0);

            if (OwnValues != null)
            {
                foreach (var value in OwnValues)
                    list.Add(value.Key);
            }

            if (Blittable == null)
                return list;

            foreach (var prop in Blittable.GetPropertyNames())
            {
                if (Deletes?.Contains(prop) == true)
                    continue;
                if (OwnValues != null && OwnValues.ContainsKey(prop))
                    continue;

                list.Add(prop);
            }

            return list;
        }

        private void RecordNumericFieldType(string key, BlittableJsonToken type)
        {
            OriginalPropertiesTypes ??= new Dictionary<string, BlittableJsonToken>();
            OriginalPropertiesTypes[key] = type;
        }

        public void Reset()
        {
            if (OwnValues == null)
                return;

            foreach (var val in OwnValues)
            {
                if (val.Value.Value is BlittableObjectInstance boi)
                    boi.Blittable.Dispose();
            }
        }
    }
}
