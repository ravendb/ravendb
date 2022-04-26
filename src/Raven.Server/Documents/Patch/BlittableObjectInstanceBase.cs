//using System;
//using System.Collections.Generic;
//using System.Linq;
//using Lucene.Net.Store;
//using Raven.Server.Documents.Indexes;
//using Raven.Server.Documents.Queries.Results;
//using Sparrow.Json;
//// ReSharper disable InconsistentNaming

//namespace Raven.Server.Documents.Patch;

//public abstract class BlittableObjectInstanceBase<T> : IBlittableObjectInstance<T> 
//    where T : struct, IJsHandle<T>
//{
//    protected bool _set;

//    protected HashSet<string> _deletes;

//    public IJsEngineHandle<T> EngineHandle { get; set; }

//    protected Dictionary<string, IBlittableObjectProperty<T>> _ownValues;
//    public Dictionary<string, IBlittableObjectProperty<T>> OwnValues
//    {
//        get => _ownValues;
//        set => _ownValues = value;
//    }
//    protected bool _changed;
//    public bool Changed => _changed;

//    protected DateTime? _lastModified;
//    public DateTime? LastModified => _lastModified;

//    protected string _changeVector;
//    public string ChangeVector => _changeVector;

//    protected BlittableJsonReaderObject _blittable;
//    public BlittableJsonReaderObject Blittable => _blittable;

//    protected string _documentId;
//    public string DocumentId => _documentId;

//    protected Dictionary<string, BlittableJsonToken> _originalPropertiesTypes;
//    public Dictionary<string, BlittableJsonToken> OriginalPropertiesTypes => _originalPropertiesTypes;

//    protected Lucene.Net.Documents.Document _luceneDocument;
//    public Lucene.Net.Documents.Document LuceneDocument
//    {
//        get { return _luceneDocument; }
//        set { _luceneDocument = value; }
//    }

//    protected IState _luceneState;
//    public IState LuceneState
//    {
//        get { return _luceneState; }
//        set { _luceneState = value; }
//    }

//    protected Dictionary<string, IndexField> _luceneIndexFields;
//    public Dictionary<string, IndexField> LuceneIndexFields
//    {
//        get { return _luceneIndexFields; }
//        set { _luceneIndexFields = value; }
//    }

//    protected bool _luceneAnyDynamicIndexFields;
//    public bool LuceneAnyDynamicIndexFields
//    {
//        get { return _luceneAnyDynamicIndexFields; }
//        set { _luceneAnyDynamicIndexFields = value; }
//    }

//    public ProjectionOptions _projection;
//    public ProjectionOptions Projection
//    {
//        get { return _projection; }
//        set { _projection = value; }
//    }

//    protected BlittableObjectInstanceBase<T> _parent;
//    public bool IsRoot => _parent == null;

//    protected Document _doc;
//    public SpatialResult? Distance => _doc?.Distance;
//    public float? IndexScore => _doc?.IndexScore;

//    protected BlittableObjectInstanceBase(IJsEngineHandle<T> engine, BlittableObjectInstanceBase<T> parent, BlittableJsonReaderObject blittable,
//        string id, DateTime? lastModified, string changeVector)
//    {
//        EngineHandle = engine;
//        _parent = parent;
//        blittable.NoCache = true;
//        _lastModified = lastModified;
//        _changeVector = changeVector;
//        _blittable = blittable;
//        _documentId = id;
//    }

//    protected BlittableObjectInstanceBase(IJsEngineHandle<T> engine, BlittableObjectInstanceBase<T> parent, BlittableJsonReaderObject blittable, Document doc)
//        : this(engine, parent, blittable, doc.Id, doc.LastModified, doc.ChangeVector)
//    {
//        _doc = doc;
//    }

//    public abstract T CreateJsHandle(bool keepAlive = false);
//    //TODO: add in derived: property._changed = true;
//    public abstract IBlittableObjectProperty<T> GenerateProperty(string propertyName);
//    public abstract T GetOwnPropertyJs(string propertyName);
//    public abstract T SetOwnProperty(string propertyName, T jsValue, bool toReturnCopy = true);
//    public abstract bool? DeleteOwnProperty(string propertyName);

//    public bool TryGetValue(string propertyName, out IBlittableObjectProperty<T> value, out bool isDeleted)
//    {
//        value = null;
//        isDeleted = _deletes?.Contains(propertyName) == true;
//        if (isDeleted)
//            return false;

//        if (_ownValues?.TryGetValue(propertyName, out IBlittableObjectProperty<T> property) == true)
//        {
//            value = property;
//            return true;
//        }

//        return false;
//    }

//    public T GetOrCreate(string key)
//    {
//        IBlittableObjectProperty<T> property = default;
//        if (_ownValues?.TryGetValue(key, out property) == true &&
//            property != null)
//            return property.Value.Clone();

//        property = GenerateProperty(key);
//        MarkChanged();
//        _ownValues ??= new Dictionary<string, IBlittableObjectProperty<T>>(_blittable.Count);

//        _ownValues[key] = property;
//        _deletes?.Remove(key);

//        return property.Value.Clone();
//    }

//    public IEnumerable<string> EnumerateOwnProperties()
//    {
//        return EnumerateOwnPropertiesUnordered().OrderBy(s => s); // TODO [shlomo] transfer Jint fix for issue RavenDB-17312
//    }

//    public void Reset()
//    {
//        if (_ownValues == null)
//            return;

//        foreach (var val in _ownValues.Values)
//        {

//            if (val.Value.AsObject() is BlittableObjectInstanceBase<T> boi)
//                boi._blittable.Dispose();
//        }
//    }

//    protected void MarkChanged()
//    {
//        _changed = true;
//        _parent?.MarkChanged();
//    }

//    private IEnumerable<string> EnumerateOwnPropertiesUnordered()
//    {
//        if (_ownValues != null)
//        {
//            foreach (var value in _ownValues)
//                yield return value.Key;
//        }

//        if (_blittable != null)
//        {
//            foreach (var key in _blittable.GetPropertyNames())
//            {
//                if (_deletes?.Contains(key) == true)
//                    continue;
//                if (_ownValues?.ContainsKey(key) == true)
//                    continue;

//                yield return key;
//            }
//        }
//    }

//    public void Dispose()
//    {
//        //TODO: egor
//    }
//}
