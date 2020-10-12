using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
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
using Sparrow.Json;

namespace Raven.Server.Documents.Patch
{
    [DebuggerDisplay("Blittable JS object")]
    public class BlittableObjectInstance : ObjectInstance
    {
        public bool Changed;
        private readonly BlittableObjectInstance _parent;
        private readonly Document _doc;
        private bool _set;

        public readonly DateTime? LastModified;
        public readonly string ChangeVector;
        public readonly BlittableJsonReaderObject Blittable;
        public readonly string DocumentId;
        public HashSet<JsValue> Deletes;
        public Dictionary<JsValue, BlittableObjectProperty> OwnValues;
        public Dictionary<string, BlittableJsonToken> OriginalPropertiesTypes;
        public Lucene.Net.Documents.Document LuceneDocument;
        public IState LuceneState;
        public Dictionary<string, IndexField> LuceneIndexFields;
        public bool LuceneAnyDynamicIndexFields;

        public ProjectionOptions Projection;

        public SpatialResult? Distance => _doc?.Distance;
        public float? IndexScore => _doc?.IndexScore;

        private void MarkChanged()
        {
            Changed = true;
            _parent?.MarkChanged();
        }

        public ObjectInstance GetOrCreate(JsValue key)
        {
            BlittableObjectProperty property = default;
            if (OwnValues?.TryGetValue(key, out property) == true &&
                property != null)
                return property.Value.AsObject();

            property = GenerateProperty(key.AsString());

            OwnValues ??= new Dictionary<JsValue, BlittableObjectProperty>(Blittable.Count);

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

                if (TryGetValueFromLucene(_parent, _property, out _value) == false)
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

            private bool TryGetValueFromLucene(BlittableObjectInstance parent, string property, out JsValue value)
            {
                value = null;

                if (parent.Projection?.MustExtractFromDocument == true)
                    return false;

                if (parent.LuceneDocument == null || parent.LuceneIndexFields == null)
                    return false;

                if (parent.LuceneIndexFields.TryGetValue(_property, out var indexField) == false && parent.LuceneAnyDynamicIndexFields == false)
                    return false;

                if (indexField != null && indexField.Storage == FieldStorage.No)
                    return false;

                var fieldType = QueryResultRetrieverBase.GetFieldType(property, parent.LuceneDocument);
                if (fieldType.IsArray)
                {
                    // here we need to perform a manipulation in order to generate the object from the data
                    if (fieldType.IsJson)
                    {
                        Lucene.Net.Documents.Field[] propertyFields = parent.LuceneDocument.GetFields(property);

                        JsValue[] arrayItems =
                            new JsValue[propertyFields.Length];

                        for (int i = 0; i < propertyFields.Length; i++)
                        {
                            var field = propertyFields[i];
                            var stringValue = field.StringValue(parent.LuceneState);

                            var itemAsBlittable = parent.Blittable._context.ReadForMemory(stringValue, field.Name);

                            arrayItems[i] = TranslateToJs(parent, field.Name, BlittableJsonToken.StartObject, itemAsBlittable);
                        }

                        value = FromObject(parent.Engine, arrayItems);
                        return true;
                    }

                    var values = parent.LuceneDocument.GetValues(property, parent.LuceneState);
                    value = FromObject(parent.Engine, values);
                    return true;
                }

                var fieldable = _parent.LuceneDocument.GetFieldable(property);
                if (fieldable == null)
                    return false;

                var val = fieldable.StringValue(_parent.LuceneState);
                if (fieldType.IsJson)
                {
                    BlittableJsonReaderObject valueAsBlittable = parent.Blittable._context.ReadForMemory(val, property);
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

        public override bool Delete(JsValue property)
        {
            if (Deletes == null)
                Deletes = new HashSet<JsValue>();

            var desc = GetOwnProperty(property);

            if (desc == PropertyDescriptor.Undefined)
                return true;

            MarkChanged();
            Deletes.Add(property);
            return OwnValues?.Remove(property) == true;
        }

        public override PropertyDescriptor GetOwnProperty(JsValue property)
        {
            BlittableObjectProperty val = default;
            if (OwnValues?.TryGetValue(property, out val) == true &&
                val != null)
                return val;

            Deletes?.Remove(property);

            val = new BlittableObjectProperty(this, property.AsString());

            if (val.Value.IsUndefined() &&
                DocumentId == null &&
                _set == false)
            {
                return PropertyDescriptor.Undefined;
            }

            OwnValues ??= new Dictionary<JsValue, BlittableObjectProperty>(Blittable.Count);

            OwnValues[property] = val;

            return val;
        }

        public override bool Set(JsValue property, JsValue value, JsValue receiver)
        {
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
                JsValue key = prop;
                if (Deletes?.Contains(key) == true)
                    continue;
                if (OwnValues?.ContainsKey(prop) == true)
                    continue;
                yield return new KeyValuePair<JsValue, PropertyDescriptor>(
                    prop,
                    GetOwnProperty(key)
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
                JsValue key = prop;
                if (Deletes?.Contains(key) == true)
                    continue;
                if (OwnValues != null && OwnValues.ContainsKey(key))
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
    }
}
