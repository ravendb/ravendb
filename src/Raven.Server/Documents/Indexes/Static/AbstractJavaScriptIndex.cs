using System;
using System.Collections.Generic;
using System.Linq;
using Esprima;
using Jint;
using Jint.Native.Function;
using Jint.Native.Object;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide;
using Sparrow.Server;
using V8.Net;

namespace Raven.Server.Documents.Indexes.Static;

public abstract class AbstractJavaScriptIndex<T> : AbstractJavaScriptIndexBase
    where T : struct, IJsHandle<T>
{
    public IJavaScriptUtils<T> JsUtils;
    public JavaScriptIndexUtils<T> JsIndexUtils;

    public Engine _engineForParsing; // is used for maps static analysis, but not for running
    public ObjectInstance _definitionsForParsing;

    public IJsEngineHandle<T> EngineHandle;
    protected T _definitions;


    protected AbstractJavaScriptIndex(IndexDefinition definition)
        : base(definition)
    {
    }

    public abstract IDisposable DisableConstraintsOnInit();

    protected void Initialize(Action<List<string>> modifyMappingFunctions, string mapCode, long indexVersion)
    {
        using (DisableConstraintsOnInit())
        {
            var maps = GetMappingFunctions(modifyMappingFunctions);

            var mapReferencedCollections = InitializeEngine(maps, mapCode);

            //TODO: egor why we have both of them??? Cant we use only _definitions?
            _definitionsForParsing = GetDefinitionsForParsingJint();
            _definitions = GetDefinitions();

            ProcessMaps(maps, mapReferencedCollections, out var collectionFunctions);

            ProcessReduce(indexVersion);

            ProcessFields(collectionFunctions);
        }
    }

    private void ProcessFields(Dictionary<string, Dictionary<string, List<JavaScriptMapOperation<T>>>> collectionFunctions)
    {
        var fields = new HashSet<string>();
        HasDynamicFields = false;
        foreach (var (collection, vals) in collectionFunctions)
        {
            foreach (var (subCollection, val) in vals)
            {
                //TODO: Validation of matches fields between group by / collections / etc
                foreach (var operation in val)
                {
                    AddMapInternal(collection, subCollection, (IndexingFunc)operation.IndexingFunction);

                    HasDynamicFields |= operation.HasDynamicReturns;
                    HasBoostedFields |= operation.HasBoostedFields;

                    fields.UnionWith(operation.Fields);
                    foreach (var (k, v) in operation.FieldOptions)
                    {
                        Definition.Fields.Add(k, v);
                    }
                }
            }
        }

        if (Definition.Fields != null)
        {
            foreach (var item in Definition.Fields)
            {
                if (string.Equals(item.Key, Constants.Documents.Indexing.Fields.AllFields))
                    continue;

                fields.Add(item.Key);
            }
        }

        OutputFields = fields.ToArray();
    }

    ~AbstractJavaScriptIndex()
    {
        _definitions.Dispose();
    }
    public JavaScriptReduceOperation<T> ReduceOperation { get; protected set; }

    //TODO: egor uncomment usage of those methods in tests
    public void SetBufferPoolForTestingPurposes(UnmanagedBuffersPoolWithLowMemoryHandling bufferPool)
    {
        ReduceOperation?.SetBufferPoolForTestingPurposes(bufferPool);
    }

    public void SetAllocatorForTestingPurposes(ByteStringContext byteStringContext)
    {
        ReduceOperation?.SetAllocatorForTestingPurposes(byteStringContext);
    }

    protected class MapMetadata
    {
        public HashSet<CollectionName> ReferencedCollections;

        public bool HasCompareExchangeReferences;
    }
    //TODO: egor this should be abstract and devided into 2 methods? (_definitions & _definitionsForParsing) the I can use it normally to create JavaScriptReduceOperation
    private void ProcessReduce(long indexVersion)
    {
        using (var reduceObj = _definitions.GetProperty(ReduceProperty))
        {
            if (!reduceObj.IsUndefined && reduceObj.IsObject)
            {
                var reduceObjForParsingJint = _definitionsForParsing.GetProperty(ReduceProperty)?.Value;
                if (reduceObjForParsingJint != null && reduceObjForParsingJint.IsObject())
                {
                    var reduceAsObjForParsingJint = reduceObjForParsingJint?.AsObject();
                    var groupByKeyForParsingJint = reduceAsObjForParsingJint?.GetProperty(KeyProperty).Value.As<ScriptFunctionInstance>();
                    if (groupByKeyForParsingJint == null)
                    {
                        throw new ArgumentException("Failed to get reduce key object");
                    }

                    var groupByKey = reduceObj.GetProperty(KeyProperty);
                    var reduce = reduceObj.GetProperty(AggregateByProperty);
                        //using (var groupByKey = reduceObj.GetProperty(KeyProperty))
                        //using (var reduce = reduceObj.GetProperty(AggregateByProperty))
                        //{
                        ReduceOperation = CreateJavaScriptReduceOperation(groupByKeyForParsingJint, reduce, groupByKey, indexVersion);
                //    }

                    GroupByFields = ReduceOperation.GetReduceFieldsNames();
                    Reduce = ReduceOperation.IndexingFunction;
                }
                else
                    throw new ArgumentException("Failed to get the reduce object: ");
            }
        }
    }

    public abstract JavaScriptReduceOperation<T> CreateJavaScriptReduceOperation(ScriptFunctionInstance groupByKeyForParsingJint, T reduce, T groupByKey, long indexVersion);

    protected abstract void ProcessMaps(List<string> mapList, List<MapMetadata> mapReferencedCollections, out Dictionary<string, Dictionary<string, List<JavaScriptMapOperation<T>>>> collectionFunctions);

    protected virtual void OnInitializeEngine()
    {
        var loadFunc = EngineHandle.CreateClrCallBack(Load, LoadDocument);

        var noTrackingObject = EngineHandle.CreateObject();
        noTrackingObject.FastAddProperty(Load, loadFunc.Clone(), writable: false, enumerable: false, configurable: false);
        EngineHandle.SetGlobalProperty(NoTracking, noTrackingObject);

        EngineHandle.SetGlobalProperty(Load, loadFunc);
        EngineHandle.SetGlobalClrCallBack(CmpXchg, LoadCompareExchangeValue);
        EngineHandle.SetGlobalClrCallBack("tryConvertToNumber", TryConvertToNumber);
        EngineHandle.SetGlobalClrCallBack("recurse", Recurse);
    }

    protected abstract List<MapMetadata> InitializeEngine(List<string> maps, string mapCode);

    private T LoadDocument(T self, T[] args)
    {
        if (args.Length != 2)
        {
            throw new ArgumentException("The load(id, collection) method expects two arguments, but got: " + args.Length);
        }

        if (args[0].IsNull || args[0].IsUndefined)
            return EngineHandle.Null;

        if (args[0].IsStringEx == false ||
            args[1].IsStringEx == false)
        {
            throw new ArgumentException($"The load(id, collection) method expects two string arguments, but got: load({args[0]}, {args[1]})");
        }

        object doc = CurrentIndexingScope.Current.LoadDocument(null, args[0].AsString, args[1].AsString);
        if (!(doc is DynamicNullObject) && JsIndexUtils.GetValue(doc, out var itemHandle))
            return itemHandle;

        return EngineHandle.Null;
    }

    private T LoadCompareExchangeValue(T self, T[] args)
    {
        if (args.Length != 1)
            throw new ArgumentException("The cmpxchg(key) method expects one argument, but got: " + args.Length);

        var keyArgument = args[0];
        if (keyArgument.IsNull || keyArgument.IsUndefined)
            return EngineHandle.Null;

        if (keyArgument.IsStringEx)
        {
            object value = CurrentIndexingScope.Current.LoadCompareExchangeValue(null, keyArgument.AsString);
            return ConvertToJsHandle(value);
        }
        else if (keyArgument.IsArray)
        {
            int arrayLength = keyArgument.ArrayLength;
            if (arrayLength == 0)
                return EngineHandle.Null;

            var jsItems = new T[arrayLength];
            for (int i = 0; i < arrayLength; i++)
            {
                using (var key = keyArgument.GetProperty(i))
                {
                    if (key.IsStringEx == false)
                        ThrowInvalidType(key, JSValueType.String);

                    object value = CurrentIndexingScope.Current.LoadCompareExchangeValue(null, key.AsString);
                    jsItems[i] = ConvertToJsHandle(value);
                }
            }

            return EngineHandle.CreateArray(jsItems);
        }
        else
        {
            throw new InvalidOperationException($"Argument '{keyArgument}' was of type '{keyArgument.ValueType}', but either string or array of strings was expected.");
        }

        static void ThrowInvalidType(T value, JSValueType expectedType)
        {
            throw new InvalidOperationException($"Argument '{value}' was of type '{value.ValueType}', but '{expectedType}' was expected.");
        }
    }

    private T TryConvertToNumber(T self, T[] args)
    {
        if (args.Length != 1)
        {
            throw new ArgumentException("The tryConvertToNumber(value) method expects one argument, but got: " + args.Length);
        }

        var value = args[0];

        if (value.IsNull || value.IsUndefined)
            return EngineHandle.Null;

        if (value.IsNumber)
            return value;

        if (value.IsStringEx)
        {
            var valueAsString = value.AsString;
            if (double.TryParse(valueAsString, out var valueAsDbl))
            {
                return EngineHandle.CreateValue(valueAsDbl);
            }
        }

        return EngineHandle.Null;
    }

    private T Recurse(T self, T[] args)
    {
        if (args.Length != 2)
        {
            throw new ArgumentException("The recurse(item, func) method expects two arguments, but got: " + args.Length);
        }


        return GetRecursiveJsFunctionInternal(args);
    }

    public abstract T ConvertToJsHandle(object value);
    public abstract T GetRecursiveJsFunctionInternal(T[] args);

    protected MapMetadata CollectReferencedCollections(string code, string additionalSources)
    {
        var javascriptParser = new JavaScriptParser(code, DefaultParserOptions);
        var program = javascriptParser.ParseScript();
        var loadVisitor = new EsprimaReferencedCollectionVisitor();
        if (string.IsNullOrEmpty(additionalSources) == false)
        {
            try
            {
                loadVisitor.Visit(new JavaScriptParser(additionalSources, DefaultParserOptions).ParseScript());
            }
            catch { }
        }

        try
        {
            loadVisitor.Visit(program);
        }
        catch { }

        return new MapMetadata
        {
            ReferencedCollections = loadVisitor.ReferencedCollection,
            HasCompareExchangeReferences = loadVisitor.HasCompareExchangeReferences
        };
    }

    private T GetDefinitions()
    {
        var definitions = EngineHandle.GetGlobalProperty(GlobalDefinitions);

        if (definitions.IsNull || definitions.IsUndefined || definitions.IsObject == false)
            ThrowIndexCreationException($"is missing its '{GlobalDefinitions}' global variable, are you modifying it in your script?");

        if (definitions.GetProperty(MapsProperty).IsUndefined)
            ThrowIndexCreationException($"is missing its '{MapsProperty}' property, are you modifying it in your script?");

        return definitions;
    }

    public ObjectInstance GetDefinitionsForParsingJint()
    {
        var definitionsObj = _engineForParsing.GetValue(GlobalDefinitions);

        if (definitionsObj.IsNull() || definitionsObj.IsUndefined() || definitionsObj.IsObject() == false)
            ThrowIndexCreationException($"is missing its '{GlobalDefinitions}' global variable, are you modifying it in your script?");

        var definitions = definitionsObj.AsObject();
        if (definitions.HasProperty(MapsProperty) == false)
            ThrowIndexCreationException("is missing its 'globalDefinition.maps' property, are you modifying it in your script?");

        return definitions;
    }
}
