using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using V8.Net;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.JavaScript;
using Raven.Server.Documents.Queries.Results;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;
using Raven.Server.Utils;
using Raven.Server.Extensions;

namespace Raven.Server.Documents.Patch
{
    [DebuggerDisplay("Blittable JS object")]
    //[ScriptObject("BlittableObjectInstance", ScriptMemberSecurity.NoAcccess)]
    public class BlittableObjectInstance : IDisposable
    {
        public class CustomBinder : ObjectBinderEx<BlittableObjectInstance>
        {
            public CustomBinder() : base()
            {
            }

            public override InternalHandle NamedPropertyGetter(ref string propertyName)
            {
                return ObjCLR.GetOwnPropertyJs(propertyName);
            }

            public override InternalHandle NamedPropertySetter(ref string propertyName, InternalHandle jsValue, V8PropertyAttributes attributes = V8PropertyAttributes.Undefined)
            {
                return ObjCLR.SetOwnProperty(propertyName, jsValue, attributes);
            }

            public override bool? NamedPropertyDeleter(ref string propertyName)
            {
                return ObjCLR.DeleteOwnProperty(propertyName);
            }

            public override V8PropertyAttributes? NamedPropertyQuery(ref string propertyName)
            {
                /*V8PropertyAttributes? res = base.NamedPropertyQuery(ref propertyName);
                if (res != null)
                    return res;*/

                return ObjCLR.QueryOwnProperty(propertyName);
            }

            public override InternalHandle NamedPropertyEnumerator()
            {
                return ObjCLR.EnumerateOwnPropertiesJs();
            }

        }

        private bool _disposed = false;

        public JavaScriptUtils JavaScriptUtils;
        public V8EngineEx Engine;

        public bool Changed;
        private BlittableObjectInstance _parent;
        private bool _isEngineRooted;
        private Document _doc;
        private bool _set;

        public DateTime? LastModified;
        public string ChangeVector;
        public BlittableJsonReaderObject Blittable;
        public string DocumentId;
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

        private int HandleID; // just for debugging
        private Int32 ObjectID; // just for debugging

        public InternalHandle CreateObjectBinder(bool keepAlive = false) {
            return BlittableObjectInstance.CreateObjectBinder(Engine, this, keepAlive: keepAlive);
        }

        public static InternalHandle CreateObjectBinder(V8EngineEx engine, BlittableObjectInstance boi, bool keepAlive = false)
        {
            InternalHandle jsBinder = engine.CreateObjectBinder<BlittableObjectInstance.CustomBinder>(boi, engine.TypeBinderBlittableObjectInstance, keepAlive: keepAlive);

            boi.HandleID = jsBinder.ID;
            boi.ObjectID = jsBinder.ObjectID;
            GC.SuppressFinalize(boi);

            return jsBinder;
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
            string changeVector
        )
        {
            JavaScriptUtils = javaScriptUtils;
            Engine = (V8EngineEx)JavaScriptUtils.Engine;

            _parent = parent;
            _isEngineRooted = false;
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
            //GC.SuppressFinalize(this); // seems to be ignored here
        }

        ~BlittableObjectInstance()
        {            
            Dispose(false);
        }


        public void Dispose()
        {  
            Dispose(true);
        }

        protected void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            if (Deletes != null) {
                Deletes.Clear();
            }

            if (OwnValues != null) {
                foreach (var val in OwnValues.Values) {
                    val.Dispose();
                }
                OwnValues.Clear();
            }


            if (OriginalPropertiesTypes != null) {
                OriginalPropertiesTypes.Clear();
            }

            LuceneDocument = null;
            LuceneState = null;

            if (LuceneIndexFields != null) {
                LuceneIndexFields.Clear();
            }

            if (disposing) {
                JavaScriptUtils = null;
                Engine = null;

                _parent = null;
                _doc = null;

                LastModified = null;
                Blittable = null;
                
                LuceneIndexFields = null;
                OriginalPropertiesTypes = null;
                Projection = null;

                Deletes = null;
                OwnValues = null;

                //GC.SuppressFinalize(this);
            }

            _disposed = true;
        }


        bool IsRooted
        {
            get {
                return _isEngineRooted || (_parent?._isEngineRooted ?? false);
            }
        }

        public InternalHandle GetOwnPropertyJs(string propertyName)
        {
            var desc = GetOwnProperty(propertyName);
            if (desc != null) {
                return desc.ValueCopy();
            }
            return InternalHandle.Empty;
        }

        private void _CheckIsNotDisposed(string descCtx)
        {
            if (_disposed) {
                throw new InvalidOperationException($"BOI has been disposed: DocumentId={DocumentId}, HandleID={HandleID}, ObjectID={ObjectID}, context: {descCtx}");
            }
        }

        public BlittableObjectProperty GetOwnProperty(string propertyName)
        {
            _CheckIsNotDisposed($"GetOwnProperty: ${propertyName}");

            BlittableObjectProperty val = null;
            if (OwnValues?.TryGetValue(propertyName, out val) == true &&
                val != null)
            {
                return val;
            }

            if (propertyName == Constants.Documents.Metadata.Key && IsRoot()) {
                var scope = CurrentIndexingScope.Current;
                scope.RegisterJavaScriptUtils(JavaScriptUtils);
                GetMetadata();
                OwnValues?.TryGetValue(propertyName, out val);
                return val;
            }
            
            Deletes?.Remove(propertyName);

            /*var propertyIndex = Blittable.GetPropertyIndex(propertyName);
            if (propIndex == -1)
                return null;*/

            val = new BlittableObjectProperty(this, propertyName);
            //GC.SuppressFinalize(val);

            if (val.Value.IsEmpty &&
                DocumentId == null &&
                _set == false)
            {
                val.Dispose();
                return null;
            }

            OwnValues ??= new Dictionary<string, BlittableObjectProperty>(Blittable.Count);

            OwnValues[propertyName] = val;

            return val;
        }

        public InternalHandle SetOwnProperty(string propertyName, InternalHandle jsValue, V8PropertyAttributes attributes = V8PropertyAttributes.Undefined, bool toReturnCopy = true)
        {
            _CheckIsNotDisposed($"SetOwnProperty: ${propertyName}");

            _set = true;
            try
            {
                BlittableObjectProperty val = null;
                if (OwnValues?.TryGetValue(propertyName, out val) == true &&
                    val != null)
                {
                    val.Value = jsValue;
                    return toReturnCopy ? val.ValueCopy() : val.Value;
                }
                
                Deletes?.Remove(propertyName);

                val = new BlittableObjectProperty(this, propertyName, jsValue);
                //GC.SuppressFinalize(val);
                val.Changed = true;
                MarkChanged();
                OwnValues ??= new Dictionary<string, BlittableObjectProperty>(Blittable.Count);
                OwnValues[propertyName] = val;
                return toReturnCopy ? val.ValueCopy() : val.Value;
            }
            finally
            {
                _set = false;
            }
        }

        public bool? DeleteOwnProperty(string propertyName)
        {
            _CheckIsNotDisposed($"DeleteOwnProperty: ${propertyName}");

            if (Deletes == null)
                Deletes = new HashSet<string>();

            var desc = GetOwnProperty(propertyName);
            if (desc == null)
                return InternalHandle.Empty;

            MarkChanged();
            Deletes.Add(propertyName);
            return OwnValues?.Remove(propertyName);
        }

        public V8PropertyAttributes? QueryOwnProperty(string propertyName)
        {
            if (OwnValues?.ContainsKey(propertyName) == true || Array.IndexOf(Blittable.GetPropertyNames(), propertyName) >= 0)
                return V8PropertyAttributes.None;

            return null;
        }

        public IEnumerable<string> EnumerateOwnPropertiesAux()
        {
            _CheckIsNotDisposed($"EnumerateOwnPropertiesAux");

            if (OwnValues != null)
            {
                foreach (var value in OwnValues)
                    yield return value.Key;
            }

            if (Blittable != null) {
                foreach (var key in Blittable.GetPropertyNames())
                {
                    if (Deletes?.Contains(key) == true)
                        continue;
                    if (OwnValues?.ContainsKey(key) == true)
                        continue;

                    yield return key;
                }
            }
        }

        public IEnumerable<string> EnumerateOwnProperties()
        {
            return EnumerateOwnPropertiesAux().OrderBy(s => s); //.ToList().Sort();
        }

        public InternalHandle EnumerateOwnPropertiesJs()
        {
            _CheckIsNotDisposed($"EnumerateOwnPropertiesJs");

            var list = Engine.CreateArray(Array.Empty<InternalHandle>());
            void pushKey(string value) {
                using (var jsResPush = list.StaticCall("push", Engine.CreateValue(value)))
                    jsResPush.ThrowOnError();
            }

            IEnumerable<string> propertyNames = EnumerateOwnProperties();
            foreach (var propertyName in propertyNames)
                pushKey(propertyName);

            /*if (OwnValues != null)
            {
                foreach (var value in OwnValues)
                    pushKey(value.Key);
            }

            if (Blittable == null) {
                return list;
            }

            foreach (var key in Blittable.GetPropertyNames())
            {
                if (Deletes?.Contains(key) == true)
                    continue;
                if (OwnValues?.ContainsKey(key) == true)
                    continue;

                pushKey(key);
            }*/

            //using (var jsStrList2 = Engine.JsonStringify.StaticCall(list)) var strList1 = jsStrList2.AsString;
            return list;
        }

        public bool IsRoot()
        {
            return _parent == null;
        }

        public InternalHandle GetMetadata()
        {
            _CheckIsNotDisposed($"GetMetadata");

            try {
                var propertyName = Constants.Documents.Metadata.Key;
                if (!(Blittable[propertyName] is BlittableJsonReaderObject metadata))
                    return Engine.CreateNullValue();

                metadata.Modifications = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.ChangeVector] = ChangeVector,
                    [Constants.Documents.Metadata.Id] = DocumentId,
                    [Constants.Documents.Metadata.LastModified] = LastModified,
                };

                if (IndexScore != null)
                    metadata.Modifications[Constants.Documents.Metadata.IndexScore] = IndexScore.Value;

                if (Distance != null)
                    metadata.Modifications[Constants.Documents.Metadata.SpatialResult] = Distance.Value.ToJson();

                // we cannot dispose the metadata here because the BOI is accessing blittable directly using the .Blittable property
                //using (var old = metadata)
                {
                    metadata = JavaScriptUtils.Context.ReadObject(metadata, DocumentId);
                    using (InternalHandle metadataJs = JavaScriptUtils.TranslateToJs(JavaScriptUtils.Context, metadata, false))
                    {
                        if (metadataJs.IsError)
                            return metadataJs;
                        return SetOwnProperty(propertyName, metadataJs, toReturnCopy: false);
                    }
                }
            }
            catch (Exception e) 
            {
                return Engine.CreateError(e.Message, JSValueType.ExecutionError);
            }
        }

        public InternalHandle GetOrCreate(InternalHandle key)
        {
            return GetOrCreate(key.AsString);
        }

        public InternalHandle GetOrCreate(string strKey)
        {
            _CheckIsNotDisposed($"GetOrCreate: ${strKey}");

            BlittableObjectProperty property = null;
            if (OwnValues?.TryGetValue(strKey, out property) == true &&
                property != null) 
            {
                return property.ValueCopy();
            }

            property = GenerateProperty(strKey);

            OwnValues ??= new Dictionary<string, BlittableObjectProperty>(Blittable.Count);

            OwnValues[strKey] = property;
            Deletes?.Remove(strKey);

            return property.ValueCopy();


            BlittableObjectProperty GenerateProperty(string propertyName)
            {
                var propertyIndex = Blittable.GetPropertyIndex(propertyName);
                BlittableObjectProperty prop = null;
                //GC.SuppressFinalize(prop);
                if (propertyIndex == -1) {
                    using (var jsValue = Engine.CreateObject())
                        prop = new BlittableObjectProperty(this, propertyName, jsValue);
                }
                else {
                    prop = new BlittableObjectProperty(this, propertyName);
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

        public sealed class BlittableObjectProperty : IDisposable
        {
            private bool _disposed = false;
    
            private BlittableObjectInstance _parent;
            private string _propertyName;

            public JavaScriptUtils JavaScriptUtils;
            public V8EngineEx Engine;
            private InternalHandle _value = InternalHandle.Empty;
            public bool Changed;

             // just for debugging
            private string DocumentId; 
            private int HandleID;
            private int ObjectID;

            public string Name
            {
                get => _propertyName;
            }

            public InternalHandle Value
            {
                get 
                {
                    _CheckIsNotDisposed($"Value");
                    return _value;
                }

                set
                {
                    if (_value.Equals(value))
                        return;
                    _value.Set(value);
                    _OnSetValue();
                    _parent.MarkChanged();
                    Changed = true;
                }
            }

            public InternalHandle ValueCopy()
            {
                _CheckIsNotDisposed($"ValueCopy");

                if (_value.IsEmpty) {
                    return InternalHandle.Empty;
                }
                return new InternalHandle(ref _value, true);
            }

            private void _OnSetValue()
            {
                if (!_value.IsEmpty) {
                    HandleID = _value.ID;
                    ObjectID = _value.ObjectID;
                }
            }

            private void Init(BlittableObjectInstance parent, string propertyName)
            {
                _parent = parent;
                DocumentId = _parent.DocumentId;
                _propertyName = propertyName;
                JavaScriptUtils = _parent.JavaScriptUtils;
                Engine = _parent.Engine;

                HandleID = -1;
                ObjectID = -1;

                GC.SuppressFinalize(this);
            }

            public BlittableObjectProperty(BlittableObjectInstance parent, string propertyName, InternalHandle jsValue)
            {
                Init(parent, propertyName);
                _value = new InternalHandle(ref jsValue, true);
                _OnSetValue();
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
                _OnSetValue();
            }

            ~BlittableObjectProperty()
            {
                Dispose(false);
            }

            public void Dispose()
            {  
                Dispose(true);
            }

            protected void Dispose(bool disposing)
            {
                if (_disposed)
                    return;

#if DEBUG
                if (_value.IsEmpty && HandleID >= 0)
                    throw new InvalidOperationException($"Property's internal handle is empty on disposal: propertyName={_propertyName}, handleID={HandleID}, objectID={ObjectID}, parentHandleID={_parent.HandleID}, parentObjectID={_parent.ObjectID}");
#endif                

                if (disposing) {
                    // releasing managed resources
                    _parent = null;

                    JavaScriptUtils = null;
                    Engine = null;

                    //GC.SuppressFinalize(this); 
                }
                
                // releasing unmanaged resources
                // ...
                _value.Dispose();

                _disposed = true;
            }

            private void _CheckIsNotDisposed(string descCtx)
            {
                if (_disposed) {
                    throw new InvalidOperationException($"BlittableObjectProperty has been disposed: DocumentId={DocumentId}, propertyName={_propertyName}, HandleID={HandleID}, ObjectID={ObjectID}, context: {descCtx}");
                }
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
                            jsValue = Engine.ExplicitNull.CreateHandle();
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
                        return Engine.ExplicitNull.CreateHandle();

                    case BlittableJsonToken.Boolean:
                        return Engine.CreateValue((bool)value);

                    case BlittableJsonToken.Integer:
                        // TODO: in the future, add [numeric type]TryFormat, when parsing numbers to strings
                        owner?.RecordNumericFieldType(key, BlittableJsonToken.Integer);
                        return Engine.CreateValue((double)(Int64)value);

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
                                return boi.CreateObjectBinder(false);
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
                return engine.CreateObjectBinder(value, keepAlive: false);
            }
        }
    }
}
