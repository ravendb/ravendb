using System;
using System.Runtime.CompilerServices;
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
using Raven.Server.Documents.Indexes.Static.JavaScript.Jint;
using Raven.Server.Documents.Queries.Results;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using TypeConverter = Raven.Server.Utils.TypeConverter;

namespace Raven.Server.Documents.Patch.Jint
{
    [DebuggerDisplay("Blittable JS object")]
    public class BlittableObjectInstanceJint : ObjectInstance, IBlittableObjectInstance, IObjectInstance<JsHandleJint>
    {
        private JintEngineEx _engineEx;

        private bool _changed;
        private readonly BlittableObjectInstanceJint _parent;
        private readonly Document _doc;
        private bool _set;

        private readonly DateTime? _lastModified;
        private readonly string _changeVector;
        private readonly BlittableJsonReaderObject _blittable;
        private readonly string _documentId;
        private HashSet<JsValue> _deletes;
        public Dictionary<JsValue, BlittableObjectProperty> _ownValues;
        private Dictionary<string, BlittableJsonToken> _originalPropertiesTypes;
        private Lucene.Net.Documents.Document _luceneDocument;
        private IState _luceneState;
        private Dictionary<string, IndexField> _luceneIndexFields;
        private bool _luceneAnyDynamicIndexFields;

        private ProjectionOptions _projection;

        public Engine EngineJint => _engine; 
        public JintEngineEx EngineExJint => _engineEx; 
        public Dictionary<JsValue, BlittableObjectProperty> OwnValues => _ownValues;

        public IJsEngineHandle<JsHandleJint> EngineHandle => _engineEx;
        public bool Changed => _changed;
        public DateTime? LastModified => _lastModified;
        public string ChangeVector => _changeVector;
        public BlittableJsonReaderObject Blittable => _blittable;
        public string DocumentId => _documentId;
        public Dictionary<string, BlittableJsonToken> OriginalPropertiesTypes => _originalPropertiesTypes;

        public Lucene.Net.Documents.Document LuceneDocument
        {
            get { return _luceneDocument; }
            set { _luceneDocument = value; }
        }
        public IState LuceneState
        {
            get { return _luceneState; }
            set { _luceneState = value; }
        }
        public Dictionary<string, IndexField> LuceneIndexFields
        {
            get { return _luceneIndexFields; }
            set { _luceneIndexFields = value; }
        }
        public bool LuceneAnyDynamicIndexFields
        {
            get { return _luceneAnyDynamicIndexFields; }
            set { _luceneAnyDynamicIndexFields = value; }
        }
        public ProjectionOptions Projection
        {
            get { return _projection; }
            set { _projection = value; }
        }

        public void Dispose()
        {}
        
        public bool IsRoot => _parent == null;
        public SpatialResult? Distance => _doc?.Distance;
        public float? IndexScore => _doc?.IndexScore;
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandleJint GetOrCreate(string key)
        {
            return new JsHandleJint(GetOrCreateJint(key));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandleJint GetOwnPropertyJs(string propertyName)
        {
            return new JsHandleJint(GetOwnPropertyJsJint(propertyName));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandleJint SetOwnProperty(string propertyName, JsHandleJint jsValue, bool toReturnCopy = true)
        {
            return new JsHandleJint(Set(propertyName, jsValue.Item, this));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool? DeleteOwnProperty(string propertyName)
        {
            return Delete(propertyName);
        }

        private void MarkChanged()
        {
            _changed = true;
            _parent?.MarkChanged();
        }

        public JsHandleJint CreateJsHandle(bool keepAlive = false)
        {
            return new JsHandleJint(this);
        }
        
        public bool TryGetValue(string propertyName, out IBlittableObjectProperty<JsHandleJint> value, out bool isDeleted)
        {
            value = null;
            isDeleted = _deletes?.Contains(propertyName) == true;
            if (isDeleted)
                return false;

            BlittableObjectProperty property = null;
            if (_ownValues?.TryGetValue(propertyName, out property) == true)
            {
                value = property;
                return true;
            }
            return false;
        }

        public ObjectInstance GetOrCreateJint(JsValue key)
        {
            BlittableObjectProperty property = default;
            if (_ownValues?.TryGetValue(key, out property) == true &&
                property != null)
                return property.Value.AsObject();

            property = GenerateProperty(key.AsString());

            _ownValues ??= new Dictionary<JsValue, BlittableObjectProperty>(_blittable.Count);

            _ownValues[key] = property;
            _deletes?.Remove(key);

            return property.Value.AsObject();

            BlittableObjectProperty GenerateProperty(string propertyName)
            {
                var propertyIndex = _blittable.GetPropertyIndex(propertyName);

                var prop = new BlittableObjectProperty(this, propertyName);
                if (propertyIndex == -1)
                {
                    prop.Value = new ObjectInstance(_engine);
                }

                return prop;
            }
        }

        public sealed class BlittableObjectProperty : PropertyDescriptor, IBlittableObjectProperty<JsHandleJint>
        {
            private readonly BlittableObjectInstanceJint _parent;
            private readonly string _property;
            private JsValue _value;
            public bool _changed;

            public override string ToString()
            {
                return _property;
            }

            public BlittableObjectProperty(BlittableObjectInstanceJint parent, string property)
                : base(PropertyFlag.CustomJsValue | PropertyFlag.Writable | PropertyFlag.WritableSet | PropertyFlag.Enumerable | PropertyFlag.EnumerableSet)
            {
                _parent = parent;
                _property = property;

                if (TryGetValueFromLucene(_parent, _property, out _value) == false)
                {
                    if (_parent._projection?.MustExtractFromIndex == true)
                    {
                        if (_parent._projection.MustExtractOrThrow)
                            _parent._projection.ThrowCouldNotExtractFieldFromIndexBecauseIndexDoesNotContainSuchFieldOrFieldValueIsNotStored(property);

                        _value = JsValue.Undefined;
                        return;
                    }

                    if (TryGetValueFromDocument(_parent, _property, out _value) == false)
                    {
                        if (_parent._projection?.MustExtractFromDocument == true)
                        {
                            if (_parent._projection.MustExtractOrThrow)
                                _parent._projection.ThrowCouldNotExtractFieldFromDocumentBecauseDocumentDoesNotContainSuchField(_parent._documentId, property);
                        }

                        _value = JsValue.Undefined;
                    }
                }
            }

            private bool TryGetValueFromDocument(BlittableObjectInstanceJint parent, string property, out JsValue value)
            {
                value = null;

                var index = parent._blittable?.GetPropertyIndex(property);
                if (index == null || index == -1)
                    return false;

                var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();

                parent._blittable.GetPropertyByIndex(index.Value, ref propertyDetails, true);

                value = TranslateToJs(parent, property, propertyDetails.Token, propertyDetails.Value);
                return true;
            }

            private bool TryGetValueFromLucene(BlittableObjectInstanceJint parent, string property, out JsValue value)
            {
                value = null;

                if (parent._projection?.MustExtractFromDocument == true)
                    return false;

                if (parent._luceneDocument == null || parent._luceneIndexFields == null)
                    return false;

                if (parent._luceneIndexFields.TryGetValue(_property, out var indexField) == false && parent._luceneAnyDynamicIndexFields == false)
                    return false;

                if (indexField != null && indexField.Storage == FieldStorage.No)
                    return false;

                var fieldType = QueryResultRetrieverBase.GetFieldType(property, parent._luceneDocument);
                if (fieldType.IsArray)
                {
                    // here we need to perform a manipulation in order to generate the object from the data
                    if (fieldType.IsJson)
                    {
                        Lucene.Net.Documents.Field[] propertyFields = parent._luceneDocument.GetFields(property);

                        JsValue[] arrayItems =
                            new JsValue[propertyFields.Length];

                        for (int i = 0; i < propertyFields.Length; i++)
                        {
                            var field = propertyFields[i];
                            var stringValue = field.StringValue(parent._luceneState);

                            var itemAsBlittable = parent._blittable._context.Sync.ReadForMemory(stringValue, field.Name);

                            arrayItems[i] = TranslateToJs(parent, field.Name, BlittableJsonToken.StartObject, itemAsBlittable);
                        }

                        value = FromObject(parent._engine, arrayItems);
                        return true;
                    }

                    var values = parent._luceneDocument.GetValues(property, parent._luceneState);
                    value = FromObject(parent._engine, values);
                    return true;
                }

                var fieldable = _parent._luceneDocument.GetFieldable(property);
                if (fieldable == null)
                    return false;

                var val = fieldable.StringValue(_parent._luceneState);
                if (fieldType.IsJson)
                {
                    BlittableJsonReaderObject valueAsBlittable = parent._blittable._context.Sync.ReadForMemory(val, property);
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
                            value = DynamicJsNullJint.ExplicitNullJint;
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
                                                            $"documentId = '{parent._documentId}', value = {val}.");
                    }
                }
                else
                {
                    value = val;
                }

                return true;
            }

            public bool Changed => _changed;
            public JsHandleJint ValueHandle => new JsHandleJint(CustomValue);
            
            public void Dispose()
            {}

            protected override JsValue CustomValue
            {
                get => _value;
                set
                {
                    if (Equals(value, _value))
                        return;
                    _value = value;
                    _parent.MarkChanged();
                    _changed = true;
                }
            }

            private ArrayInstance GetArrayInstanceFromBlittableArray(Engine e, BlittableJsonReaderArray bjra, BlittableObjectInstanceJint parent)
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
                jsArray.SetPrototypeOf(e.Realm.Intrinsics.Array.PrototypeObject);

                return jsArray;
            }

            private JsValue TranslateToJs(BlittableObjectInstanceJint owner, string key, BlittableJsonToken type, object value)
            {
                switch (type & BlittableJsonReaderBase.TypesMask)
                {
                    case BlittableJsonToken.Null:
                        return DynamicJsNullJint.ExplicitNullJint;

                    case BlittableJsonToken.Boolean:
                        return (bool)value ? JsBoolean.True : JsBoolean.False;

                    case BlittableJsonToken.Integer:
                        // TODO: in the future, add [numeric type]TryFormat, when parsing numbers to strings
                        owner?.RecordNumericFieldType(key, BlittableJsonToken.Integer);
                        return (long)value;

                    case BlittableJsonToken.LazyNumber:
                        owner?.RecordNumericFieldType(key, BlittableJsonToken.LazyNumber);
                        return GetJsValueForLazyNumber(owner?._engine, (LazyNumberValue)value);

                    case BlittableJsonToken.String:
                        return value.ToString();

                    case BlittableJsonToken.CompressedString:
                        return value.ToString();

                    case BlittableJsonToken.StartObject:
                        _changed = true;
                        _parent.MarkChanged();
                        BlittableJsonReaderObject blittable = (BlittableJsonReaderObject)value;

                        var obj = TypeConverter.TryConvertBlittableJsonReaderObject(blittable);
                        switch (obj)
                        {
                            case BlittableJsonReaderArray blittableArray:
                                return GetArrayInstanceFromBlittableArray(owner._engine, blittableArray, owner);

                            case LazyStringValue asLazyStringValue:
                                return asLazyStringValue.ToString();

                            case LazyCompressedStringValue asLazyCompressedStringValue:
                                return asLazyCompressedStringValue.ToString();

                            default:
                                blittable.NoCache = true;
                                return new BlittableObjectInstanceJint(owner._engine,
                                    owner,
                                    blittable, null, null, null);
                        }

                    case BlittableJsonToken.StartArray:
                        _changed = true;
                        _parent.MarkChanged();
                        var array = (BlittableJsonReaderArray)value;
                        return GetArrayInstanceFromBlittableArray(owner._engine, array, owner);

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

        public BlittableObjectInstanceJint(Engine engine,
            BlittableObjectInstanceJint parent,
            BlittableJsonReaderObject blittable,
            string id,
            DateTime? lastModified,
            string changeVector) : base(engine)
        {
            //_engine = engine;
          //  _engineEx = (JintEngineEx)engine;
            _parent = parent;
            blittable.NoCache = true;
            _lastModified = lastModified;
            _changeVector = changeVector;
            _blittable = blittable;
            _documentId = id;

            SetPrototypeOf(engine.Realm.Intrinsics.Object.PrototypeObject);
        }

        public BlittableObjectInstanceJint(Engine engine,
            BlittableObjectInstanceJint parent,
            BlittableJsonReaderObject blittable,
            Document doc) : this(engine, parent, blittable, doc.Id, doc.LastModified, doc.ChangeVector)
        {
            _doc = doc;
        }

        public override bool Delete(JsValue property)
        {
            if (_deletes == null)
                _deletes = new HashSet<JsValue>();

            var desc = GetOwnProperty(property);

            if (desc == PropertyDescriptor.Undefined)
                return true;

            MarkChanged();
            _deletes.Add(property);
            return _ownValues?.Remove(property) == true;
        }

        public JsValue GetOwnPropertyJsJint(JsValue property)
        {
            return GetOwnProperty(property).Value;
        }

        public override PropertyDescriptor GetOwnProperty(JsValue property)
        {
            BlittableObjectProperty val = default;
            if (_ownValues?.TryGetValue(property, out val) == true &&
                val != null)
                return val;

            _deletes?.Remove(property);

            val = new BlittableObjectProperty(this, property.AsString());

            if (val.Value.IsUndefined() &&
                _documentId == null &&
                _set == false)
            {
                return PropertyDescriptor.Undefined;
            }

            _ownValues ??= new Dictionary<JsValue, BlittableObjectProperty>(_blittable.Count);

            _ownValues[property] = val;

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
            if (_ownValues != null)
            {
                foreach (var value in _ownValues)
                {
                    yield return new KeyValuePair<JsValue, PropertyDescriptor>(value.Key, value.Value);
                }
            }

            if (_blittable == null)
                yield break;

            foreach (var prop in _blittable.GetPropertyNames())
            {
                JsValue key = prop;
                if (_deletes?.Contains(key) == true)
                    continue;
                if (_ownValues?.ContainsKey(prop) == true)
                    continue;
                yield return new KeyValuePair<JsValue, PropertyDescriptor>(
                    prop,
                    GetOwnProperty(key)
                    );
            }
        }

        public IEnumerable<string> EnumerateOwnProperties()
        {
            if (_ownValues != null)
            {
                foreach (var value in _ownValues)
                    yield return value.Key.AsString();
            }

            if (_blittable != null)
            {
                foreach (var prop in _blittable.GetPropertyNames())
                {
                    string key = prop;
                    if (_deletes?.Contains(key) == true)
                        continue;
                    if (_ownValues != null && _ownValues.ContainsKey(key))
                        continue;

                    yield return prop;
                }
            }
        }
        
        public override List<JsValue> GetOwnPropertyKeys(Types types)
        {
            var list = new List<JsValue>(_blittable?.Count ?? _ownValues?.Count ?? 0);

            if (_ownValues != null)
            {
                foreach (var value in _ownValues)
                    list.Add(value.Key);
            }

            if (_blittable == null)
                return list;

            foreach (var prop in _blittable.GetPropertyNames())
            {
                JsValue key = prop;
                if (_deletes?.Contains(key) == true)
                    continue;
                if (_ownValues != null && _ownValues.ContainsKey(key))
                    continue;

                list.Add(prop);
            }

            return list;
        }

        private void RecordNumericFieldType(string key, BlittableJsonToken type)
        {
            _originalPropertiesTypes ??= new Dictionary<string, BlittableJsonToken>();
            _originalPropertiesTypes[key] = type;
        }

        public void Reset()
        {
            if (_ownValues == null)
                return;

            foreach (var val in _ownValues)
            {
                if (val.Value.Value is BlittableObjectInstanceJint boi)
                    boi._blittable.Dispose();
            }
        }
    }
}
