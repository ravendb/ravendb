using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using V8.Net;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static.JavaScript;
using Raven.Server.Documents.Queries.Results;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Patch
{
    [DebuggerDisplay("Blittable JS object")]
    //[ScriptObject("BlittableObjectInstance", ScriptMemberSecurity.NoAcccess)]
    public class BlittableObjectInstance
    {
        public readonly JavaScriptUtils JavaScriptUtils;
        public readonly V8EngineEx Engine;

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
        public Lucene.Net.Documents.Document LuceneDocument;
        public IState LuceneState;
        public Dictionary<string, IndexField> LuceneIndexFields;
        public bool LuceneAnyDynamicIndexFields;

        public ProjectionOptions Projection;

        public SpatialResult? Distance => _doc?.Distance;
        public float? IndexScore => _doc?.IndexScore;


        public InternalHandle CreateObjectBinder() {
            return BlittableObjectInstance.CreateObjectBinder(Engine, this);
        }

        public static InternalHandle CreateObjectBinder(V8EngineEx engine, BlittableObjectInstance boi) {
            return engine.CreateObjectBinder<BlittableObjectInstance.CustomBinder>(boi, engine.TypeBinderBlittableObjectInstance);
        }

        private void MarkChanged()
        {
            Changed = true;
            _parent?.MarkChanged();
        }

        public BlittableObjectInstance(JavaScriptUtils javaScriptUtils,
            BlittableObjectInstance parent,
            BlittableJsonReaderObject blittable,
            string id,
            DateTime? lastModified,
            string changeVector) // : base(engine)
        {
            JavaScriptUtils = javaScriptUtils;
            Engine = (V8EngineEx)JavaScriptUtils.Engine;

            _parent = parent;
            blittable.NoCache = true;
            LastModified = lastModified;
            ChangeVector = changeVector;
            Blittable = blittable;
            DocumentId = id;
        }

        public BlittableObjectInstance(JavaScriptUtils javaScriptUtils,
            BlittableObjectInstance parent,
            BlittableJsonReaderObject blittable,
            Document doc) : this(javaScriptUtils, parent, blittable, doc.Id, doc.LastModified, doc.ChangeVector)
        {
            _doc = doc;
        }

        public class CustomBinder : ObjectBinderEx<BlittableObjectInstance>
        {
            private BlittableObjectProperty GetOwnProperty(string propertyName)
            {
                /*if (propertyName == Constants.Documents.Metadata.Key)
                { 
                    using (var jsValue = ObjCLR.JavaScriptUtils.GetMetadata((V8EngineEx)Engine, false, InternalHandle.Empty, this))
                    {
                        jsValue.ThrowOnError(); // TODO check if is needed here
                        return new BlittableObjectProperty(ObjCLR, propertyName, jsValue);
                    }
                }*/

                BlittableObjectProperty val = null;
                if (ObjCLR.OwnValues?.TryGetValue(propertyName, out val) == true &&
                    val != null)
                    return val;

                ObjCLR.Deletes?.Remove(propertyName);

                val = new BlittableObjectProperty(ObjCLR, propertyName);

                if (val.Value.IsUndefined &&
                    ObjCLR.DocumentId == null &&
                    ObjCLR._set == false)
                {
                    return null;
                }

                ObjCLR.OwnValues ??= new Dictionary<string, BlittableObjectProperty>(ObjCLR.Blittable.Count);

                ObjCLR.OwnValues[propertyName] = val;

                return val;
            }

            public override InternalHandle NamedPropertyGetter(ref string propertyName)
            {
                var desc = GetOwnProperty(propertyName);
                if (desc != null)
                    return new InternalHandle(desc.Value, true);

                return base.NamedPropertyGetter(ref propertyName);
            }

            public override InternalHandle NamedPropertySetter(ref string propertyName, InternalHandle value, V8PropertyAttributes attributes = V8PropertyAttributes.Undefined)
            {
                if (propertyName == Constants.Documents.Metadata.Key) {
                    ObjCLR._set = false;
                    return InternalHandle.Empty;
                }
                
                ObjCLR._set = true;
                try
                {
                    return base.NamedPropertySetter(ref propertyName, value, attributes);
                }
                finally
                {
                    ObjCLR._set = false;
                }
            }

            public override bool? NamedPropertyDeleter(ref string propertyName)
            {
                bool? res = base.NamedPropertyDeleter(ref propertyName);
                if (res == true)
                    return res;

                if (propertyName == Constants.Documents.Metadata.Key) {
                    return false;
                }
                
                if (ObjCLR.Deletes == null)
                    ObjCLR.Deletes = new HashSet<string>();

                var desc = GetOwnProperty(propertyName);

                if (desc == null)
                    return InternalHandle.Empty;

                ObjCLR.MarkChanged();
                ObjCLR.Deletes.Add(propertyName);
                return ObjCLR.OwnValues?.Remove(propertyName);
            }

            public override V8PropertyAttributes? NamedPropertyQuery(ref string propertyName)
            {
                V8PropertyAttributes? res = base.NamedPropertyQuery(ref propertyName);
                if (res != null)
                    return res;

                if (Array.IndexOf(ObjCLR.Blittable.GetPropertyNames(), propertyName) >= 0)
                    return V8PropertyAttributes.None;

                return null;
            }

            public override InternalHandle NamedPropertyEnumerator()
            {
                var list = base.NamedPropertyEnumerator();
                void pushKey(string value) {
                    using (var jsValue = Engine.CreateValue(value))
                    using (var jsResPush = list.Call("push", InternalHandle.Empty, jsValue))
                        jsResPush.ThrowOnError(); // TODO check if is needed here
                }

                if (ObjCLR.OwnValues != null)
                {
                    foreach (var value in ObjCLR.OwnValues)
                        pushKey(value.Key);
                }

                if (ObjCLR.Blittable == null)
                    return list;

                foreach (var key in ObjCLR.Blittable.GetPropertyNames())
                {
                    if (ObjCLR.Deletes?.Contains(key) == true)
                        continue;
                    if (ObjCLR.OwnValues?.ContainsKey(key) == true)
                        continue;

                    pushKey(key);
                }

                //pushKey(Constants.Documents.Metadata.Key);

                return list;
            }

        }

        public InternalHandle GetOrCreate(InternalHandle key)
        {
            return GetOrCreate(key.AsString);
        }

        public InternalHandle GetOrCreate(string strKey)
        {
            BlittableObjectProperty property = null;
            if (OwnValues?.TryGetValue(strKey, out property) == true &&
                property != null) 
            {
                if (property.Value.IsEmpty) {
                    throw new InvalidOperationException($"property's {property.Name} value has got disposed");
                }
                return new InternalHandle(property.Value, true);
            }

            property = GenerateProperty(strKey);

            OwnValues ??= new Dictionary<string, BlittableObjectProperty>(Blittable.Count);

            OwnValues[strKey] = property;
            Deletes?.Remove(strKey);

            return new InternalHandle(property.Value, true);


            BlittableObjectProperty GenerateProperty(string propertyName)
            {
                var propertyIndex = Blittable.GetPropertyIndex(propertyName);

                var prop = new BlittableObjectProperty(this, propertyName);
                if (propertyIndex == -1)
                {
                    using (var jsValue = Engine.CreateObject())
                        prop.Value = jsValue;
                }

                return prop;
            }
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

            foreach (var val in OwnValues.Values)
            {
                if (val.Value.BoundObject is BlittableObjectInstance boi)
                    boi.Blittable.Dispose();
            }
        }

        public sealed class BlittableObjectProperty
        {
            private BlittableObjectInstance _parent;
            private string _propertyName;

            public JavaScriptUtils JavaScriptUtils;
            public V8EngineEx Engine;
            private InternalHandle _value = InternalHandle.Empty;
            public bool Changed;

            public string Name
            {
                get => _propertyName;
            }

            public InternalHandle Value
            {
                get => _value;
                set
                {
                    if (_value.Equals(value))
                        return;
                    _value.Set(value);
                    _parent.MarkChanged();
                    Changed = true;
                }
            }


            private void Init(BlittableObjectInstance parent, string propertyName)
            {
                _parent = parent;
                _propertyName = propertyName;
                JavaScriptUtils = _parent.JavaScriptUtils;
                Engine = _parent.Engine;
            }

            public BlittableObjectProperty(BlittableObjectInstance parent, string propertyName, InternalHandle jsValue)
            {
                Init(parent, propertyName);
                _value = jsValue;
            }

            public BlittableObjectProperty(BlittableObjectInstance parent, string propertyName)
            {
                Init(parent, propertyName);

                if (TryGetValueFromLucene(_parent, _propertyName, out _value) == false)
                {
                    if (_parent.Projection?.MustExtractFromIndex == true)
                    {
                        if (_parent.Projection.MustExtractOrThrow)
                            _parent.Projection.ThrowCouldNotExtractFieldFromIndexBecauseIndexDoesNotContainSuchFieldOrFieldValueIsNotStored(propertyName);

                        _value = InternalHandle.Empty;
                        return;
                    }

                    if (TryGetValueFromDocument(_parent, _propertyName, out _value) == false)
                    {
                        if (_parent.Projection?.MustExtractFromDocument == true)
                        {
                            if (_parent.Projection.MustExtractOrThrow)
                                _parent.Projection.ThrowCouldNotExtractFieldFromDocumentBecauseDocumentDoesNotContainSuchField(_parent.DocumentId, propertyName);
                        }

                        _value = InternalHandle.Empty;
                    }
                }
            }

            ~BlittableObjectProperty()
            {
                _value.Dispose();
            }

            private bool TryGetValueFromDocument(BlittableObjectInstance parent, string propertyName, out InternalHandle jsValue)
            {
                jsValue = InternalHandle.Empty;

                var index = parent.Blittable?.GetPropertyIndex(propertyName);
                if (index == null || index == -1)
                    return false;

                var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();

                parent.Blittable.GetPropertyByIndex(index.Value, ref propertyDetails, true);

                jsValue = TranslateToJs(parent, propertyName, propertyDetails.Token, propertyDetails.Value);
                return true;
            }

            private bool TryGetValueFromLucene(BlittableObjectInstance parent, string propertyName, out InternalHandle jsValue)
            {
                jsValue = InternalHandle.Empty;

                if (parent.Projection?.MustExtractFromDocument == true)
                    return false;

                if (parent.LuceneDocument == null || parent.LuceneIndexFields == null)
                    return false;

                if (parent.LuceneIndexFields.TryGetValue(_propertyName, out var indexField) == false && parent.LuceneAnyDynamicIndexFields == false)
                    return false;

                if (indexField != null && indexField.Storage == FieldStorage.No)
                    return false;

                var fieldType = QueryResultRetrieverBase.GetFieldType(propertyName, parent.LuceneDocument);
                if (fieldType.IsArray)
                {
                    // here we need to perform a manipulation in order to generate the object from the data
                    if (fieldType.IsJson)
                    {
                        Lucene.Net.Documents.Field[] propertyFields = parent.LuceneDocument.GetFields(propertyName);

                        int arrayLength =  propertyFields.Length;
                        var jsItems = new InternalHandle[arrayLength];

                        for (int i = 0; i < arrayLength; i++)
                        {
                            var field = propertyFields[i];
                            var stringValue = field.StringValue(parent.LuceneState);

                            var itemAsBlittable = parent.Blittable._context.Sync.ReadForMemory(stringValue, field.Name);

                            jsItems[i] = TranslateToJs(parent, field.Name, BlittableJsonToken.StartObject, itemAsBlittable);
                        }

                        jsValue = Engine.CreateArrayWithDisposal(jsItems);
                        return true;
                    }

                    var values = parent.LuceneDocument.GetValues(propertyName, parent.LuceneState);
                    jsValue = parent.Engine.FromObject(values);
                    return true;
                }

                var fieldable = _parent.LuceneDocument.GetFieldable(propertyName);
                if (fieldable == null)
                    return false;

                var val = fieldable.StringValue(_parent.LuceneState);
                if (fieldType.IsJson)
                {
                    BlittableJsonReaderObject valueAsBlittable = parent.Blittable._context.Sync.ReadForMemory(val, propertyName);
                    jsValue = TranslateToJs(parent, propertyName, BlittableJsonToken.StartObject, valueAsBlittable);
                    return true;
                }

                if (fieldable.IsTokenized == false)
                {
                    // NULL_VALUE and EMPTY_STRING fields aren't tokenized
                    // this will prevent converting fields with a "NULL_VALUE" string to null
                    switch (val)
                    {
                        case Client.Constants.Documents.Indexing.Fields.NullValue:
                            jsValue.Set(DynamicJsNull.ExplicitNull._);
                            return true;

                        case Client.Constants.Documents.Indexing.Fields.EmptyString:
                            jsValue = Engine.CreateValue(""); // string.Empty;
                            return true;
                    }
                }

                if (fieldType.IsNumeric)
                {
                    if (long.TryParse(val, NumberStyles.Integer, CultureInfo.InvariantCulture, out var valueAsLong))
                    {
                        jsValue = Engine.CreateValue(valueAsLong);
                    }
                    else if (double.TryParse(val, NumberStyles.Any, CultureInfo.InvariantCulture, out var valueAsDouble))
                    {
                        jsValue = Engine.CreateValue(valueAsDouble);
                    }
                    else
                    {
                        throw new InvalidOperationException($"RecoGetOwnPropertygnized field '{propertyName}' as numeric but was unable to parse its value to 'long' or 'double'. " +
                                                            $"documentId = '{parent.DocumentId}', value = {val}.");
                    }
                }
                else
                {
                    jsValue = Engine.CreateValue(val);
                }

                return true;
            }

            private InternalHandle GetArrayInstanceFromBlittableArray(V8Engine engine, BlittableJsonReaderArray bjra, BlittableObjectInstance parent)
            {
                bjra.NoCache = true;

                int arrayLength = bjra.Length;
                var jsItems = new InternalHandle[arrayLength];
                for (int i = 0; i < arrayLength; ++i)
                {
                    var json = bjra.GetValueTokenTupleByIndex(i);
                    BlittableJsonToken itemType = json.Item2 & BlittableJsonReaderBase.TypesMask;
                    jsItems[i] = (itemType == BlittableJsonToken.Integer || itemType == BlittableJsonToken.LazyNumber)
                        ? TranslateToJs(null, null, json.Item2, json.Item1)
                        : TranslateToJs(parent, null, json.Item2, json.Item1);
                }

                return Engine.CreateArrayWithDisposal(jsItems);
                /*var jsArray = engine.CreateArray(Array.Empty<InternalHandle>());
                for (var i = 0; i < bjra.Length; i++)
                {
                    var json = bjra.GetValueTokenTupleByIndex(i);
                    BlittableJsonToken itemType = json.Item2 & BlittableJsonReaderBase.TypesMask;
                    using (var item = (itemType == BlittableJsonToken.Integer || itemType == BlittableJsonToken.LazyNumber)
                        ? TranslateToJs(null, null, json.Item2, json.Item1)
                        : TranslateToJs(parent, null, json.Item2, json.Item1))
                    {
                        using (var jsResPush = jsArray.Call("push", InternalHandle.Empty, item))
                            jsResPush.ThrowOnError(); // TODO check if is needed here
                    }
                }
                return jsArray;*/
            }

            private InternalHandle TranslateToJs(BlittableObjectInstance owner, string key, BlittableJsonToken type, object value)
            {
                InternalHandle jsRes = InternalHandle.Empty;
                switch (type & BlittableJsonReaderBase.TypesMask)
                {
                    case BlittableJsonToken.Null:
                        return DynamicJsNull.ExplicitNull._;

                    case BlittableJsonToken.Boolean:
                        return Engine.CreateValue((bool)value);

                    case BlittableJsonToken.Integer:
                        // TODO: in the future, add [numeric type]TryFormat, when parsing numbers to strings
                        owner?.RecordNumericFieldType(key, BlittableJsonToken.Integer);
                        return Engine.CreateValue((Int32)value);

                    case BlittableJsonToken.LazyNumber:
                        owner?.RecordNumericFieldType(key, BlittableJsonToken.LazyNumber);
                        return GetJsValueForLazyNumber(owner?.Engine, (LazyNumberValue)value);

                    case BlittableJsonToken.String:
                        return Engine.CreateValue(value.ToString());

                    case BlittableJsonToken.CompressedString:
                        return Engine.CreateValue(value.ToString());

                    case BlittableJsonToken.StartObject:
                        Changed = true;
                        _parent.MarkChanged();
                        BlittableJsonReaderObject blittable = (BlittableJsonReaderObject)value;

                        var obj = TypeConverter.TryConvertBlittableJsonReaderObject(blittable);
                        switch (obj)
                        {
                            case BlittableJsonReaderArray blittableArray:
                                return GetArrayInstanceFromBlittableArray(owner.Engine, blittableArray, owner);

                            case LazyStringValue asLazyStringValue:
                                return Engine.CreateValue(asLazyStringValue.ToString());

                            case LazyCompressedStringValue asLazyCompressedStringValue:
                                return Engine.CreateValue(asLazyCompressedStringValue.ToString());

                            default:
                                blittable.NoCache = true;
                                var boi = new BlittableObjectInstance(owner.JavaScriptUtils,
                                    owner,
                                    blittable, null, null, null
                                );
                                return boi.CreateObjectBinder();
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

            public static InternalHandle GetJsValueForLazyNumber(V8EngineEx engine, LazyNumberValue value)
            {
 
                // First, try and see if the number is withing double boundaries.
                // We use double's tryParse and it actually may round the number,
                // But that are Jint's limitations
                if (value.TryParseDouble(out double doubleVal))
                {
                    return engine.CreateValue(doubleVal);
                }

                // If number is not in double boundaries, we return the LazyNumberValue
                return engine.CreateObjectBinder(value);
            }
        }
    }
}
