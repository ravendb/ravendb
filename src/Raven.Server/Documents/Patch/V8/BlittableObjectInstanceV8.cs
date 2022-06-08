using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;
using V8.Net;

namespace Raven.Server.Documents.Patch.V8
{
    [DebuggerDisplay("Blittable JS object")]
    public class BlittableObjectInstanceV8 : IBlittableObjectInstance<JsHandleV8>
#if DEBUG
    , IV8DebugInfo
#endif
    {
        public class CustomBinder : ObjectBinderEx<BlittableObjectInstanceV8>
        {
            public CustomBinder() : base()
            {
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public override InternalHandle NamedPropertyGetter(ref string propertyName)
            {
                try
                {
                    return ObjClr.GetOwnPropertyJsV8(propertyName);
                }
                catch (Exception e) 
                {
                    ObjClr.EngineExV8.Context.JsContext.LastException = e;
                    return ObjClr.EngineV8.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public override InternalHandle NamedPropertySetter(ref string propertyName, InternalHandle jsValue, V8PropertyAttributes attributes = V8PropertyAttributes.Undefined)
            {
                try
                {
                    return ObjClr.SetOwnProperty(propertyName, jsValue, attributes);
                }
                catch (Exception e) 
                {
                    ObjClr.EngineExV8.Context.JsContext.LastException = e;
                    return ObjClr.EngineV8.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public override bool? NamedPropertyDeleter(ref string propertyName)
            {
                try
                {
                    return ObjClr.DeleteOwnProperty(propertyName);
                }
                catch (Exception e) 
                {
                    ObjClr.EngineExV8.Context.JsContext.LastException = e;
                    return ObjClr.EngineV8.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public override V8PropertyAttributes? NamedPropertyQuery(ref string propertyName)
            {
                try
                {
                    return ObjClr.QueryOwnProperty(propertyName);
                }
                catch (Exception e) 
                {
                    ObjClr.EngineExV8.Context.JsContext.LastException = e;
                    return null;
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public override InternalHandle NamedPropertyEnumerator()
            {
                try
                {
                    return ObjClr.EnumerateOwnPropertiesJsV8();
                }
                catch (Exception e) 
                {
                    ObjClr.EngineExV8.Context.JsContext.LastException = e;
                    return ObjClr.EngineV8.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }
        }

        private bool _disposed = false;

        private JavaScriptUtilsV8 _javaScriptUtils;
        private V8EngineEx _engineEx;
        private V8Engine _engine;

        private bool _changed;
        private BlittableObjectInstanceV8 _parent;
        private bool _isEngineRooted;
        private Document _doc;
        private bool _set;

        private DateTime? _lastModified;
        private string _changeVector;
        private BlittableJsonReaderObject _blittable;
        private string _documentId;
        private HashSet<string> _deletes;
        private Dictionary<string, BlittableObjectProperty> _ownValues;
        private Dictionary<string, BlittableJsonToken> _originalPropertiesTypes;
        private Lucene.Net.Documents.Document _luceneDocument;
        private IState _luceneState;
        private Dictionary<string, IndexField> _luceneIndexFields;
        private bool _luceneAnyDynamicIndexFields;

        public ProjectionOptions _projection;


        public V8Engine EngineV8 => _engine; 
        public V8EngineEx EngineExV8 => _engineEx;
        public Dictionary<string, IBlittableObjectProperty<JsHandleV8>> OwnValues
        {
            get
            {
                if (_ownValues == null)
                    return null;
                return _ownValues.ToDictionary(x => x.Key, x =>
                {
                    if (x.Value == null)
                        return null;
                    return (IBlittableObjectProperty<JsHandleV8>)x.Value;
                });
            }
        }

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
        
        public bool IsRoot => _parent == null;
        public SpatialResult? Distance => _doc?.Distance;
        public float? IndexScore => _doc?.IndexScore;

#if DEBUG
        private V8EntityID _SelfID;
#endif

        public IJsEngineHandle<JsHandleV8> EngineHandle => _engineEx;

        public JsHandleV8 CreateJsHandle(bool keepAlive = false)
        {
            return CreateObjectBinder(keepAlive);
        }
        
        public bool TryGetValue(string propertyName, out IBlittableObjectProperty<JsHandleV8> value, out bool isDeleted)
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandleV8 GetOrCreate(string key)
        {
            var val = GetOrCreateV8(key);
            return new JsHandleV8(ref val);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandleV8 GetOwnPropertyJs(string propertyName)
        {
            var val = GetOwnPropertyJsV8(propertyName);
            return new JsHandleV8(ref val);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandleV8 SetOwnProperty(string propertyName, JsHandleV8 jsValue, bool toReturnCopy = true)
        {
            var val = SetOwnProperty(propertyName, jsValue.Item, V8PropertyAttributes.Undefined, toReturnCopy);
            return new JsHandleV8(ref val);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public JsHandleV8 CreateObjectBinder(bool keepAlive = false)
        {
            var val = CreateObjectBinder(_engineEx, this, keepAlive: keepAlive);
            return new JsHandleV8(ref val);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static InternalHandle CreateObjectBinder(V8EngineEx engine, BlittableObjectInstanceV8 boi, bool keepAlive = false)
        {

            var jsBinder = engine.Engine.CreateObjectBinder<BlittableObjectInstanceV8.CustomBinder>(boi, engine.Context.TypeBinderBlittableObjectInstance(), keepAlive: keepAlive);
            var binder = (ObjectBinder)jsBinder.Object;
            binder.ShouldDisposeBoundObject = true;
            return jsBinder;
        }

        private void MarkChanged()
        {
            _changed = true;
            _parent?.MarkChanged();
        }

        public BlittableObjectInstanceV8(JavaScriptUtilsV8 javaScriptUtils,
            BlittableObjectInstanceV8 parent,
            BlittableJsonReaderObject blittable,
            string id,
            DateTime? lastModified,
            string changeVector
        )
        {
            _javaScriptUtils = javaScriptUtils;
            _engineEx = _javaScriptUtils.EngineEx;
            _engine = _javaScriptUtils.Engine;
 
            _parent = parent;
            _isEngineRooted = false;
            blittable.NoCache = true;
            _lastModified = lastModified;
            _changeVector = changeVector;
            _blittable = blittable;
            _documentId = id;
        }

        public BlittableObjectInstanceV8(JavaScriptUtilsV8 javaScriptUtils,
            BlittableObjectInstanceV8 parent,
            BlittableJsonReaderObject blittable,
            Document doc) : this(javaScriptUtils, parent, blittable, doc.Id, doc.LastModified, doc.ChangeVector)
        {
            _doc = doc;
        }

        ~BlittableObjectInstanceV8()
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

            _disposed = true;
            
            _deletes?.Clear();

            if (_ownValues != null)
            {
                foreach (var val in _ownValues?.Values)
                {
                    val?.Dispose();
                }
                _ownValues?.Clear();
            }

            _originalPropertiesTypes?.Clear();

            _luceneDocument = null;
            _luceneState = null;

            if (disposing)
            {
                _javaScriptUtils = null;
                _engineEx = null;

                _parent = null;
                _doc = null;

                _lastModified = null;
                _blittable = null;
                
                _luceneIndexFields = null;
                _originalPropertiesTypes = null;
                _projection = null;

                _deletes = null;
                _ownValues = null;
            }
        }


        bool IsRooted
        {
            get
            {
                return _isEngineRooted || (_parent?._isEngineRooted ?? false);
            }
        }

#if DEBUG
        public V8EntityID SelfID
        {
            get
            {
                return _SelfID;
            }
            set
            {
                _SelfID = value;
            }
        }

        public V8EntityID ParentID
        {
            get
            {
                return new V8EntityID(_parent?.SelfID?.HandleID ?? -1, _parent?.SelfID?.ObjectID ?? -1);
            } 
        }

        public List<V8EntityID> ChildIDs
        {
            get
            {
                var res = new List<V8EntityID>();

                var countProps = _ownValues?.Count ?? 0;
                if (countProps <= 0)
                    return res;

                foreach (var kvp in _ownValues)
                {
                    InternalHandle h = kvp.Value.Value;
                    res.Add(new V8EntityID(h.HandleID, h.ObjectID));

                    if (!(h.IsDisposed || h.IsCLRDisposed) && h.IsArray)
                    {
                        for (int j = 0; j < h.ArrayLength; j++)
                        {
                            using (var jsItem = h.GetProperty(j))
                            {
                                res.Add(new V8EntityID(jsItem.HandleID, jsItem.ObjectID));
                            }
                        }
                    }
                }

                return res;
            } 
        }

        public string Summary
        {
            get
            {
                string desc = "";
                if (_parent != null)
                    desc = $"parentHandleID={ParentID.HandleID}, parentObjectID={ParentID.ObjectID}";
                else
                    desc = "isRoot=true";
                return desc;
            }
        }
#endif

        public InternalHandle GetOwnPropertyJsV8(string propertyName)
        {
            var desc = GetOwnProperty(propertyName);
            if (desc != null)
                return desc.Value.Clone();
            return InternalHandle.Empty;
        }

        private void _CheckIsNotDisposed(string descCtx)
        {
            if (_disposed)
            {
                string errorDesc = $"BOI has been disposed: _documentId={_documentId}: {descCtx}";
#if DEBUG
                errorDesc += $", HandleID={SelfID?.HandleID}, ObjectID={SelfID?.ObjectID}, context";                
#endif
                throw new InvalidOperationException(errorDesc);
            }
        }

        public BlittableObjectProperty GetOwnProperty(string propertyName)
        {
            _CheckIsNotDisposed($"GetOwnProperty: ${propertyName}");

            BlittableObjectProperty val = null;
            if (_ownValues?.TryGetValue(propertyName, out val) == true &&
                val != null)
            {
                return val;
            }

            var scope = CurrentIndexingScope.Current;
            if (scope != null && propertyName == Constants.Documents.Metadata.Key && IsRoot)
            {
                // TODO: egor scope.RegisterJavaScriptUtils(_javaScriptUtils);
                GetMetadata();
                _ownValues?.TryGetValue(propertyName, out val);
                return val;
            }
            
            _deletes?.Remove(propertyName);

            val = new BlittableObjectProperty(this, propertyName);

            if (val.Value.IsEmpty &&
                _documentId == null &&
                _set == false)
            {
                val.Dispose();
                return null;
            }

            _ownValues ??= new Dictionary<string, BlittableObjectProperty>(_blittable.Count);

            _ownValues[propertyName] = val;

            return val;
        }

        public InternalHandle SetOwnProperty(string propertyName, InternalHandle jsValue, V8PropertyAttributes attributes = V8PropertyAttributes.Undefined, bool toReturnCopy = true)
        {
            _CheckIsNotDisposed($"SetOwnProperty: ${propertyName}");

            _set = true;
            try
            {
                BlittableObjectProperty val = null;
                if (_ownValues?.TryGetValue(propertyName, out val) == true &&
                    val != null)
                {
                    val.Value = jsValue;
                    return toReturnCopy ? val.Value.Clone() : val.Value;
                }
                
                _deletes?.Remove(propertyName);

                val = new BlittableObjectProperty(this, propertyName, jsValue);
                //GC.SuppressFinalize(val);
                val._changed = true;
                MarkChanged();
                _ownValues ??= new Dictionary<string, BlittableObjectProperty>(_blittable.Count);
                _ownValues[propertyName] = val;
                return toReturnCopy ? val.Value.Clone() : val.Value;
            }
            finally
            {
                _set = false;
            }
        }

        public bool? DeleteOwnProperty(string propertyName)
        {
            _CheckIsNotDisposed($"DeleteOwnProperty: ${propertyName}");

            _deletes ??= new HashSet<string>();

            var val = GetOwnProperty(propertyName);
            if (val == null)
                return false;

            val.Dispose();
            MarkChanged();
            _deletes.Add(propertyName);
            _ownValues.Remove(propertyName);
            return true;
        }

        public V8PropertyAttributes? QueryOwnProperty(string propertyName)
        {
            if (_ownValues?.ContainsKey(propertyName) == true || Array.IndexOf(_blittable.GetPropertyNames(), propertyName) >= 0)
                return V8PropertyAttributes.None;

            return null;
        }

        public IEnumerable<string> EnumerateOwnPropertiesUnordered()
        {
            _CheckIsNotDisposed($"EnumerateOwnPropertiesAux");

            if (_ownValues != null)
            {
                foreach (var value in _ownValues)
                    yield return value.Key;
            }

            if (_blittable != null)
            {
                foreach (var key in _blittable.GetPropertyNames())
                {
                    if (_deletes?.Contains(key) == true)
                        continue;
                    if (_ownValues?.ContainsKey(key) == true)
                        continue;

                    yield return key;
                }
            }
        }

        public IEnumerable<string> EnumerateOwnProperties()
        {
            return EnumerateOwnPropertiesUnordered().OrderBy(s => s); // TODO [shlomo] transfer Jint fix for issue RavenDB-17312
        }

        public InternalHandle EnumerateOwnPropertiesJsV8()
        {
            _CheckIsNotDisposed($"EnumerateOwnPropertiesJsV8");

            var list = _engine.CreateArray(Array.Empty<InternalHandle>());
            void pushKey(string value)
            {
                using (var jsValue = _engine.CreateValue(value))
                using (var jsResPush = list.StaticCall("push", jsValue))
                    jsResPush.ThrowOnError();
            }

            IEnumerable<string> propertyNames = EnumerateOwnProperties();
            foreach (var propertyName in propertyNames)
                pushKey(propertyName);
            return list;
        }

        public JsHandleV8 GetMetadata(bool toReturnCopy = false)
        {
            _CheckIsNotDisposed($"GetMetadata");

            try
            {
                var propertyName = Constants.Documents.Metadata.Key;
                if (!(_blittable[propertyName] is BlittableJsonReaderObject metadata))
                    return _engineEx.CreateNullValue();

                metadata.Modifications = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.ChangeVector] = _changeVector,
                    [Constants.Documents.Metadata.Id] = _documentId,
                    [Constants.Documents.Metadata.LastModified] = _lastModified,
                };

                if (IndexScore != null)
                    metadata.Modifications[Constants.Documents.Metadata.IndexScore] = IndexScore.Value;

                if (Distance != null)
                    metadata.Modifications[Constants.Documents.Metadata.SpatialResult] = Distance.Value.ToJson();

                // we cannot dispose the metadata here because the BOI is accessing blittable directly using the ._blittable property
                //using (var old = metadata)
                {
                    metadata = _javaScriptUtils.Context.ReadObject(metadata, _documentId);
                    var jsMetadata = _javaScriptUtils.TranslateToJs(_javaScriptUtils.Context, metadata, keepAlive: false, parent: this);
                    if (jsMetadata.IsError)
                        return jsMetadata;
                    using (jsMetadata)
                        return SetOwnProperty(propertyName, jsMetadata, toReturnCopy: toReturnCopy);
                }
            }
            catch (Exception e) 
            {
                return _engineEx.CreateError(e.ToString(), JSValueType.ExecutionError);
            }
        }

        public InternalHandle GetOrCreate(InternalHandle key)
        {
            return GetOrCreateV8(key.AsString);
        }

        public InternalHandle GetOrCreateV8(string strKey)
        {
            _CheckIsNotDisposed($"GetOrCreate: ${strKey}");

            BlittableObjectProperty property = null;
            if (_ownValues?.TryGetValue(strKey, out property) == true &&
                property != null) 
            {
                return property.Value.Clone();
            }

            property = GenerateProperty(strKey);

            property._changed = true;
            MarkChanged();

            _ownValues ??= new Dictionary<string, BlittableObjectProperty>(_blittable.Count);

            _ownValues[strKey] = property;
            _deletes?.Remove(strKey);

            return property.Value.Clone();


            BlittableObjectProperty GenerateProperty(string propertyName)
            {
                var propertyIndex = _blittable.GetPropertyIndex(propertyName);
                BlittableObjectProperty prop = null;
                //GC.SuppressFinalize(prop);
                if (propertyIndex == -1)
                {
                    using (var jsValue = _engine.CreateObject())
                        prop = new BlittableObjectProperty(this, propertyName, jsValue);
                }
                else
                    prop = new BlittableObjectProperty(this, propertyName);
                return prop;
            }
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

            foreach (var val in _ownValues.Values)
            {
                
                if (val.Value.BoundObject is BlittableObjectInstanceV8 boi)
                    boi._blittable.Dispose();
            }
        }

        public sealed class BlittableObjectProperty : IBlittableObjectProperty<JsHandleV8>
#if DEBUG
    , IV8DebugInfo
#endif
        {
            private bool _disposed;
    
            private BlittableObjectInstanceV8 _parent;
            private string _propertyName;

            private JavaScriptUtilsV8 _javaScriptUtils;
            private V8EngineEx _engineEx;
            private V8Engine _engine;
            private InternalHandle _value = InternalHandle.Empty;
            public bool _changed;

            private string _documentId; 

#if DEBUG
            private V8EntityID _SelfID;
#endif

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
                    _changed = true;
                }
            }

            private void _OnSetValue()
            {
#if DEBUG
                if (!_value.IsEmpty)
                    _SelfID = new V8EntityID(_value.HandleID, _value.ObjectID);
#endif                
            }

            private void Init(BlittableObjectInstanceV8 parent, string propertyName)
            {
                _parent = parent;
                _documentId = _parent._documentId;
                _propertyName = propertyName;
                _javaScriptUtils = _parent._javaScriptUtils;
                _engineEx = _parent._engineEx;
                _engine = _parent._engine;

                GC.SuppressFinalize(this); 
            }

            public BlittableObjectProperty(BlittableObjectInstanceV8 parent, string propertyName, InternalHandle jsValue)
            {
                Init(parent, propertyName);
                _value = jsValue.Clone();
                _OnSetValue();
            }

            public BlittableObjectProperty(BlittableObjectInstanceV8 parent, string propertyName)
            {
                Init(parent, propertyName);

                if (TryGetValueFromLucene(_parent, _propertyName, out _value) == false)
                {
                    if (_parent._projection?.MustExtractFromIndex == true)
                    {
                        if (_parent._projection.MustExtractOrThrow)
                            _parent._projection.ThrowCouldNotExtractFieldFromIndexBecauseIndexDoesNotContainSuchFieldOrFieldValueIsNotStored(propertyName);

                        _value = InternalHandle.Empty;
                        return;
                    }

                    if (TryGetValueFromDocument(_parent, _propertyName, out _value) == false)
                    {
                        if (_parent._projection?.MustExtractFromDocument == true)
                        {
                            if (_parent._projection.MustExtractOrThrow)
                                _parent._projection.ThrowCouldNotExtractFieldFromDocumentBecauseDocumentDoesNotContainSuchField(_parent._documentId, propertyName);
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

            public void Dispose(bool disposing)
            {
                if (_disposed)
                    return;

#if DEBUG
                if (_value.IsEmpty && SelfID != null && SelfID.HandleID >= 0)
                    throw new InvalidOperationException($"Property's internal handle is empty on disposal: {Summary}");
#endif

                _value.Dispose();

                if (disposing)
                {
                    // releasing managed resources
                    _parent = null;

                    _javaScriptUtils = null;
                    _engine = null;
                }
                
                _disposed = true;
            }

            public bool Changed => _changed;
            public JsHandleV8 ValueHandle
            {
                get
                {
                    InternalHandle internalHandle = Value;
                    return new JsHandleV8(ref internalHandle);
                }
            }

#if DEBUG
            public V8EntityID SelfID
            {
                get { return _SelfID; }
                set { _SelfID = value; } 
            }

            public V8EntityID ParentID
            {
                get { return _parent.SelfID; } 
            }

            public List<V8EntityID> ChildIDs
            {
                get { return null; } 
            }
#endif

            public string Summary
            {
                get
                {
                    var res = $"BlittableObjectProperty has been disposed: _documentId={_documentId}, propertyName={_propertyName}";
#if DEBUG
                    res += $", HandleID={SelfID.HandleID}, ObjectID={SelfID.ObjectID}, parentHandleID={_parent.SelfID.HandleID}, parentObjectID={_parent.SelfID.ObjectID}";
#endif
                    return res;
                }
            }

            private void _CheckIsNotDisposed(string descCtx)
            {
                if (_disposed)
                    throw new InvalidOperationException($"BlittableObjectProperty has been disposed: context: {descCtx}, {Summary}");
            }

            private bool TryGetValueFromDocument(BlittableObjectInstanceV8 parent, string propertyName, out InternalHandle jsValue)
            {
                jsValue = InternalHandle.Empty;

                var index = parent._blittable?.GetPropertyIndex(propertyName);
                if (index == null || index == -1)
                    return false;

                var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();

                parent._blittable.GetPropertyByIndex(index.Value, ref propertyDetails, true);

                jsValue = TranslateToJs(parent, propertyName, propertyDetails.Token, propertyDetails.Value);
                return true;
            }

            private bool TryGetValueFromLucene(BlittableObjectInstanceV8 parent, string propertyName, out InternalHandle jsValue)
            {
                jsValue = InternalHandle.Empty;

                if (parent._projection?.MustExtractFromDocument == true)
                    return false;

                if (parent._luceneDocument == null || parent._luceneIndexFields == null)
                    return false;

                if (parent._luceneIndexFields.TryGetValue(_propertyName, out var indexField) == false && parent._luceneAnyDynamicIndexFields == false)
                    return false;

                if (indexField != null && indexField.Storage == FieldStorage.No)
                    return false;

                var fieldType = QueryResultRetrieverBase.GetFieldType(propertyName, parent._luceneDocument);
                if (fieldType.IsArray)
                {
                    // here we need to perform a manipulation in order to generate the object from the data
                    if (fieldType.IsJson)
                    {
                        Lucene.Net.Documents.Field[] propertyFields = parent._luceneDocument.GetFields(propertyName);

                        int arrayLength =  propertyFields.Length;
                        var jsItems = new InternalHandle[arrayLength];

                        for (int i = 0; i < arrayLength; i++)
                        {
                            var field = propertyFields[i];
                            var stringValue = field.StringValue(parent._luceneState);

                            var itemAsBlittable = parent._blittable._context.Sync.ReadForMemory(stringValue, field.Name);

                            jsItems[i] = TranslateToJs(parent, field.Name, BlittableJsonToken.StartObject, itemAsBlittable);
                        }

                        jsValue = _engine.CreateArrayWithDisposal(jsItems);
                        return true;
                    }

                    var values = parent._luceneDocument.GetValues(propertyName, parent._luceneState);
                    jsValue = parent._engine.FromObject(values);
                    return true;
                }

                var fieldable = _parent._luceneDocument.GetFieldable(propertyName);
                if (fieldable == null)
                    return false;

                var val = fieldable.StringValue(_parent._luceneState);
                if (fieldType.IsJson)
                {
                    BlittableJsonReaderObject valueAsBlittable = parent._blittable._context.Sync.ReadForMemory(val, propertyName);
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
                            jsValue = _engineEx.ExplicitNull.Item;
                            return true;

                        case Client.Constants.Documents.Indexing.Fields.EmptyString:
                            jsValue = _engine.CreateValue(""); // string.Empty;
                            return true;
                    }
                }

                if (fieldType.IsNumeric)
                {
                    if (long.TryParse(val, NumberStyles.Integer, CultureInfo.InvariantCulture, out var valueAsLong))
                    {
                        jsValue = _engine.CreateValue(valueAsLong);
                    }
                    else if (double.TryParse(val, NumberStyles.Any, CultureInfo.InvariantCulture, out var valueAsDouble))
                    {
                        jsValue = _engine.CreateValue(valueAsDouble);
                    }
                    else
                    {
                        throw new InvalidOperationException($"Recognized field '{propertyName}' as numeric but was unable to parse its value to 'long' or 'double'. " +
                                                            $"value = {val}, {Summary}.");
                    }
                }
                else
                {
                    jsValue = _engine.CreateValue(val);
                }

                return true;
            }

            private InternalHandle GetArrayInstanceFromBlittableArray(V8Engine engine, BlittableJsonReaderArray bjra, BlittableObjectInstanceV8 parent)
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

                return _engine.CreateArrayWithDisposal(jsItems);
            }

            private InternalHandle TranslateToJs(BlittableObjectInstanceV8 owner, string key, BlittableJsonToken type, object value)
            {
                switch (type & BlittableJsonReaderBase.TypesMask)
                {
                    case BlittableJsonToken.Null:
                        return _engineEx.ExplicitNull.Item;

                    case BlittableJsonToken.Boolean:
                        return _engine.CreateValue((bool)value);

                    case BlittableJsonToken.Integer:
                        // TODO: in the future, add [numeric type]TryFormat, when parsing numbers to strings
                        owner?.RecordNumericFieldType(key, BlittableJsonToken.Integer);
                        return _engine.CreateValue((double)(Int64)value);

                    case BlittableJsonToken.LazyNumber:
                        owner?.RecordNumericFieldType(key, BlittableJsonToken.LazyNumber);
                        return GetJsValueForLazyNumber(_engineEx, (LazyNumberValue)value);

                    case BlittableJsonToken.String:
                        return _engine.CreateValue(value.ToString());

                    case BlittableJsonToken.CompressedString:
                        return _engine.CreateValue(value.ToString());

                    case BlittableJsonToken.StartObject:
                        _changed = true;
                        _parent.MarkChanged();
                        BlittableJsonReaderObject blittable = (BlittableJsonReaderObject)value;

                        var obj = TypeConverter.TryConvertBlittableJsonReaderObject(blittable);
                        switch (obj)
                        {
                            case BlittableJsonReaderArray blittableArray:
                                return GetArrayInstanceFromBlittableArray(_engineEx.Engine, blittableArray, owner);

                            case LazyStringValue asLazyStringValue:
                                return _engine.CreateValue(asLazyStringValue.ToString());

                            case LazyCompressedStringValue asLazyCompressedStringValue:
                                return _engine.CreateValue(asLazyCompressedStringValue.ToString());

                            default:
                                blittable.NoCache = true;
                                var boi = new BlittableObjectInstanceV8(_javaScriptUtils,
                                    owner,
                                    blittable, null, null, null
                                );
                                return boi.CreateObjectBinder(keepAlive: false).Item;
                        }

                    case BlittableJsonToken.StartArray:
                        _changed = true;
                        _parent.MarkChanged();
                        var array = (BlittableJsonReaderArray)value;
                        return GetArrayInstanceFromBlittableArray(_engineEx.Engine, array, owner);

                    default:
                        throw new ArgumentOutOfRangeException(type.ToString());
                }
            }

            public static InternalHandle GetJsValueForLazyNumber(V8EngineEx engineEx, LazyNumberValue value)
            {
 
                // First, try and see if the number is withing double boundaries.
                // We use double's tryParse and it actually may round the number,
                // But that are Jint's limitations
                if (value.TryParseDouble(out double doubleVal))
                {
                    var engine = engineEx.Engine; 
                    return engine.CreateValue(doubleVal);
                }

                // If number is not in double boundaries, we return the LazyNumberValue
                return engineEx.Engine.CreateObjectBinder(value, keepAlive: false);
            }
        }
    }
}
