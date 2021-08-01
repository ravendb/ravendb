using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.Configuration;
using Raven.Server.ServerWide;
using Sparrow.Server;

using Esprima;
using Jint;
using Jint.Native;
using Jint.Native.Function;
using Jint.Native.Object;
using Jint.Runtime;
//using Jint.Runtime.Interop;
using JintClrFunctionInstance = Jint.Runtime.Interop.ClrFunctionInstance;

using V8.Net;

using Raven.Server.Extensions;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Indexes.Static.JavaScript;
using Raven.Server.Documents.Indexes.Static.Counters;
using Raven.Server.Documents.Indexes.Static.TimeSeries;
using Raven.Server.Documents.Indexes.Static.Utils;

namespace Raven.Server.Documents.Indexes.Static
{
    public sealed class JavaScriptIndex : AbstractJavaScriptIndex
    {

        public JavaScriptIndex(IndexDefinition definition, RavenConfiguration configuration)
            : base(definition, configuration, modifyMappingFunctions: null, JavaScriptIndex.GetMapCode())
        {
        }

        private static string GetMapCode()
        {
            return @"
function map(name, lambda) {
    var map = {
        collection: name,
        method: lambda,
        moreArgs: Array.prototype.slice.call(arguments, 2)
    };
    globalDefinition.maps.push(map);
}";
        }

        protected override void OnInitializeEngine()
        {
            base.OnInitializeEngine();

            _engineJint.SetValue("getMetadata", new JintClrFunctionInstance(_engineJint, "getMetadata", MetadataForJint)); // for backward-compatibility only
            _engineJint.SetValue("metadataFor", new JintClrFunctionInstance(_engineJint, "metadataFor", MetadataForJint));
            _engineJint.SetValue("attachmentsFor", new JintClrFunctionInstance(_engineJint, "attachmentsFor", AttachmentsForJint));
            _engineJint.SetValue("timeSeriesNamesFor", new JintClrFunctionInstance(_engineJint, "timeSeriesNamesFor", TimeSeriesNamesForJint));
            _engineJint.SetValue("counterNamesFor", new JintClrFunctionInstance(_engineJint, "counterNamesFor", CounterNamesForJint));
            _engineJint.SetValue("loadAttachment", new JintClrFunctionInstance(_engineJint, "loadAttachment", LoadAttachmentJint));
            _engineJint.SetValue("loadAttachments", new JintClrFunctionInstance(_engineJint, "loadAttachments", LoadAttachmentsJint));
            _engineJint.SetValue("id", new JintClrFunctionInstance(_engineJint, "id", GetDocumentIdJint));

            _engine.GlobalObject.SetProperty("getMetadata", new ClrFunctionInstance(MetadataFor)); // for backward-compatibility only
            _engine.GlobalObject.SetProperty("metadataFor", new ClrFunctionInstance(MetadataFor));
            _engine.GlobalObject.SetProperty("attachmentsFor", new ClrFunctionInstance(AttachmentsFor));
            _engine.GlobalObject.SetProperty("timeSeriesNamesFor", new ClrFunctionInstance(TimeSeriesNamesFor));
            _engine.GlobalObject.SetProperty("counterNamesFor", new ClrFunctionInstance(CounterNamesFor));
            _engine.GlobalObject.SetProperty("loadAttachment", new ClrFunctionInstance(LoadAttachment));
            _engine.GlobalObject.SetProperty("loadAttachments", new ClrFunctionInstance(LoadAttachments));
            _engine.GlobalObject.SetProperty("id", new ClrFunctionInstance(GetDocumentId));

        }

        protected override void ProcessMaps(List<string> mapList, List<MapMetadata> mapReferencedCollections, out Dictionary<string, Dictionary<string, List<JavaScriptMapOperation>>> collectionFunctions)
        {
            var mapsArrayJint = _definitionsJint.GetProperty(MapsProperty).Value;
            if (mapsArrayJint.IsNull() || mapsArrayJint.IsUndefined() || mapsArrayJint.IsArray() == false)
                ThrowIndexCreationException($"Jint: doesn't contain any map function or '{GlobalDefinitions}.{Maps}' was modified in the script");

            var mapsJint = mapsArrayJint.AsArray();
            if (mapsJint.Length == 0)
                ThrowIndexCreationException($"Jint: doesn't contain any map functions or '{GlobalDefinitions}.{Maps}' was modified in the script");


            using (var maps = _definitions.GetProperty(MapsProperty)) {
                if (maps.IsNull || maps.IsUndefined || maps.IsArray == false)
                    ThrowIndexCreationException($"doesn't contain any map function or '{GlobalDefinitions}.{Maps}' was modified in the script");

                int mapCount = maps.ArrayLength;
                if (mapCount == 0)
                    ThrowIndexCreationException($"doesn't contain any map functions or '{GlobalDefinitions}.{Maps}' was modified in the script");

                collectionFunctions = new Dictionary<string, Dictionary<string, List<JavaScriptMapOperation>>>();
                for (int i = 0; i < mapCount; i++)
                {
                    var mapObjJint = mapsJint.Get(i.ToString());
                    if (mapObjJint.IsNull() || mapObjJint.IsUndefined() || mapObjJint.IsObject() == false)
                        ThrowIndexCreationException($"Jint: map function #{i} is not a valid object");
                    var mapJint = mapObjJint.AsObject();                        
                    if (mapJint.HasProperty(MethodProperty) == false)
                        ThrowIndexCreationException($"Jint: map function #{i} is missing its {MethodProperty} property");
                    var funcInstanceJint = mapJint.Get(MethodProperty).As<FunctionInstance>();
                    if (funcInstanceJint == null)
                        ThrowIndexCreationException($"Jint: map function #{i} {MethodProperty} property isn't a 'FunctionInstance'");

                    using (var map = maps.GetProperty(i))
                    {
                        if (map.IsNull || map.IsUndefined || map.IsObject == false)
                            ThrowIndexCreationException($"map function #{i} is not a valid object");
                        if (map.HasProperty(CollectionProperty) == false)
                            ThrowIndexCreationException($"map function #{i} is missing a collection name");
                        using (var mapCollectionStr = map.GetProperty(CollectionProperty)) {
                            if (mapCollectionStr.IsString == false)
                                ThrowIndexCreationException($"map function #{i} collection name isn't a string");
                            var mapCollection = mapCollectionStr.AsString;

                            if (collectionFunctions.TryGetValue(mapCollection, out var subCollectionFunctions) == false)
                                collectionFunctions[mapCollection] = subCollectionFunctions = new Dictionary<string, List<JavaScriptMapOperation>>();

                            if (subCollectionFunctions.TryGetValue(mapCollection, out var list) == false)
                                subCollectionFunctions[mapCollection] = list = new List<JavaScriptMapOperation>();

                            if (map.HasProperty(MethodProperty) == false)
                                ThrowIndexCreationException($"map function #{i} is missing its {MethodProperty} property");
                            using (var funcObj = map.GetProperty(MethodProperty))
                            {
                                if (funcObj.IsFunction == false)
                                    ThrowIndexCreationException($"map function #{i} collection name isn't a string");
                                var funcInstance = funcObj.Object as V8Function;
                                if (funcInstance == null)
                                    ThrowIndexCreationException($"map function #{i} {MethodProperty} property isn't a 'V8Function'");
                                var operation = new JavaScriptMapOperation(JavaScriptIndexUtils)
                                {
                                    MapFunc = funcInstanceJint,
                                    MapFuncV8 = funcInstance,
                                    IndexName = Definition.Name,
                                    MapString = mapList[i]
                                };

                                if (mapJint.HasOwnProperty(MoreArgsProperty))
                                {
                                    var moreArgsObjJint = mapJint.Get(MoreArgsProperty);
                                    if (moreArgsObjJint.IsArray())
                                    {
                                        var arrayJint = moreArgsObjJint.AsArray();
                                        if (arrayJint.Length > 0)
                                        {
                                            operation.MoreArguments = arrayJint;
                                        }
                                    }
                                }

                                operation.Analyze(_engineJint);
                                if (ReferencedCollections.TryGetValue(mapCollection, out var collectionNames) == false)
                                {
                                    collectionNames = new HashSet<CollectionName>();
                                    ReferencedCollections.Add(mapCollection, collectionNames);
                                }

                                collectionNames.UnionWith(mapReferencedCollections[i].ReferencedCollections);

                                if (mapReferencedCollections[i].HasCompareExchangeReferences)
                                    CollectionsWithCompareExchangeReferences.Add(mapCollection);

                                list.Add(operation);
                            }
                        }
                    }
                }
            }
        }

        private JsValue GetDocumentIdJint(JsValue self, JsValue[] args)
        {
            Debug.Assert(false); // this code is not to be run
            return JsValue.Null;
            /*var scope = CurrentIndexingScope.Current;
            scope.RegisterJavaScriptUtils(JavaScriptUtils);

            return JavaScriptUtils.GetDocumentId(engine, isConstructCall, self, args);*/
        }

        private JsValue AttachmentsForJint(JsValue self, JsValue[] args)
        {
            Debug.Assert(false); // this code is not to be run
            return JsValue.Null;
            /*var scope = CurrentIndexingScope.Current;
            scope.RegisterJavaScriptUtils(JavaScriptUtils);

            return JavaScriptUtils.AttachmentsFor(engine, isConstructCall, self, args);*/
        }

        private JsValue MetadataForJint(JsValue self, JsValue[] args)
        {
            Debug.Assert(false); // this code is not to be run
            return JsValue.Null;
            /*var scope = CurrentIndexingScope.Current;
            scope.RegisterJavaScriptUtils(JavaScriptUtils);

            return JavaScriptUtils.GetMetadata(engine, isConstructCall, self, args);*/
        }

        private JsValue TimeSeriesNamesForJint(JsValue self, JsValue[] args)
        {
            Debug.Assert(false); // this code is not to be run
            return JsValue.Null;
            /*var scope = CurrentIndexingScope.Current;
            scope.RegisterJavaScriptUtils(JavaScriptUtils);

            return JavaScriptUtils.GetTimeSeriesNamesFor(engine, isConstructCall, self, args);*/
        }

        private JsValue CounterNamesForJint(JsValue self, JsValue[] args)
        {
            Debug.Assert(false); // this code is not to be run
            return JsValue.Null;
            /*var scope = CurrentIndexingScope.Current;
            scope.RegisterJavaScriptUtils(JavaScriptUtils);

            return JavaScriptUtils.GetCounterNamesFor(engine, isConstructCall, self, args);*/
        }

        private JsValue LoadAttachmentJint(JsValue self, JsValue[] args)
        {
            Debug.Assert(false); // this code is not to be run
            return JsValue.Null;
            /*var scope = CurrentIndexingScope.Current;
            scope.RegisterJavaScriptUtils(JavaScriptUtils);

            return JavaScriptUtils.LoadAttachment(engine, isConstructCall, self, args);*/
        }

        private JsValue LoadAttachmentsJint(JsValue self, JsValue[] args)
        {
            Debug.Assert(false); // this code is not to be run
            return JsValue.Null;
            /*var scope = CurrentIndexingScope.Current;
            scope.RegisterJavaScriptUtils(JavaScriptUtils);

            return JavaScriptUtils.LoadAttachments(engine, isConstructCall, self, args);*/
        }


        private InternalHandle GetDocumentId(V8EngineEx engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            var scope = CurrentIndexingScope.Current;
            scope.RegisterJavaScriptUtils(JavaScriptUtils);

            return JavaScriptUtils.GetDocumentId(engine, isConstructCall, self, args);
        }

        private InternalHandle AttachmentsFor(V8EngineEx engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            var scope = CurrentIndexingScope.Current;
            scope.RegisterJavaScriptUtils(JavaScriptUtils);

            return JavaScriptUtils.AttachmentsFor(engine, isConstructCall, self, args);
        }

        private InternalHandle MetadataFor(V8EngineEx engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            var scope = CurrentIndexingScope.Current;
            scope.RegisterJavaScriptUtils(JavaScriptUtils);

            return JavaScriptUtils.GetMetadata(engine, isConstructCall, self, args);
        }

        private InternalHandle TimeSeriesNamesFor(V8EngineEx engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            var scope = CurrentIndexingScope.Current;
            scope.RegisterJavaScriptUtils(JavaScriptUtils);

            return JavaScriptUtils.GetTimeSeriesNamesFor(engine, isConstructCall, self, args);
        }

        private InternalHandle CounterNamesFor(V8EngineEx engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            var scope = CurrentIndexingScope.Current;
            scope.RegisterJavaScriptUtils(JavaScriptUtils);

            return JavaScriptUtils.GetCounterNamesFor(engine, isConstructCall, self, args);
        }

        private InternalHandle LoadAttachment(V8EngineEx engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            var scope = CurrentIndexingScope.Current;
            scope.RegisterJavaScriptUtils(JavaScriptUtils);

            return JavaScriptUtils.LoadAttachment(engine, isConstructCall, self, args);
        }

        private InternalHandle LoadAttachments(V8EngineEx engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            var scope = CurrentIndexingScope.Current;
            scope.RegisterJavaScriptUtils(JavaScriptUtils);

            return JavaScriptUtils.LoadAttachments(engine, isConstructCall, self, args);
        }
    }

    public abstract class AbstractJavaScriptIndex : AbstractStaticIndexBase
    {
        private string MapCode;
        protected const string GlobalDefinitions = "globalDefinition";
        protected const string CollectionProperty = "collection";
        protected const string MethodProperty = "method";
        protected const string MoreArgsProperty = "moreArgs";

        protected const string MapsProperty = "maps";

        private const string ReduceProperty = "reduce";
        private const string AggregateByProperty = "aggregateBy";
        private const string KeyProperty = "key";

        
        protected readonly IndexDefinition Definition;
        public readonly JavaScriptUtils JavaScriptUtils;
        public readonly JavaScriptIndexUtils JavaScriptIndexUtils;


        //private JintPreventResolvingTasksReferenceResolver _resolverJint;
        protected Engine _engineJint; // in V8 mode is used for maps static analysis, but not for running
        protected ObjectInstance _definitionsJint;

        protected readonly V8EngineEx _engine;
        protected InternalHandle _definitions;

        public JavaScriptReduceOperation ReduceOperation { get; private set; }


        internal static AbstractJavaScriptIndex Create(IndexDefinition definition, RavenConfiguration configuration)
        {
            switch (definition.SourceType)
            {
                case IndexSourceType.Documents:
                    return new JavaScriptIndex(definition, configuration);

                case IndexSourceType.TimeSeries:
                    return new TimeSeriesJavaScriptIndex(definition, configuration);

                case IndexSourceType.Counters:
                    return new CountersJavaScriptIndex(definition, configuration);

                default:
                    throw new NotSupportedException($"Not supported source type '{definition.SourceType}'.");
            }
        }

        protected AbstractJavaScriptIndex(IndexDefinition definition, RavenConfiguration configuration, Action<List<string>> modifyMappingFunctions, string mapCode)
        {
            Definition = definition;
            MapCode = mapCode;

            var indexConfiguration = new SingleIndexConfiguration(definition.Configuration, configuration);

            //_resolverJint = new JintPreventResolvingTasksReferenceResolver();
            _engineJint = new Engine(options =>
            {
                /*options.LimitRecursion(64)
                    .SetReferencesResolver(_resolverJint)
                    .MaxStatements(indexConfiguration.MaxStepsForScript)
                    .Strict(configuration.Patching.StrictMode)
                    .AddObjectConverter(new JintGuidConverter())
                    .AddObjectConverter(new JintStringConverter())
                    .AddObjectConverter(new JintEnumConverter())
                    .AddObjectConverter(new JintDateTimeConverter())
                    .AddObjectConverter(new JintTimeSpanConverter())
                    .LocalTimeZone(TimeZoneInfo.Utc);*/
            });


            // we create the engine instance directly instead of using SingleRun
            // because the index is single threaded and long lived
            _engine = new V8EngineEx();

            string[] optionsCmd = {$"use_strict={configuration.Patching.StrictMode}"}; // TODO construct from options
            _engine.SetFlagsFromCommandLine(optionsCmd);
                    //.LimitRecursion(64)
                    //.MaxStatements(indexConfiguration.MaxStepsForScript)
                    //.LocalTimeZone(TimeZoneInfo.Utc);  // -> harmony_intl_locale_info, harmony_intl_more_timezone

            //using (_engine.DisableMaxStatements())  // TODO to V8
            //{
                var maps = GetMappingFunctions(modifyMappingFunctions);

                var mapReferencedCollections = InitializeEngine(Definition, maps, mapCode);

                _definitionsJint = GetDefinitionsJint();
                _definitions = GetDefinitions();

                ProcessMaps(maps, mapReferencedCollections, out var collectionFunctions);

                ProcessReduce();

                ProcessFields(collectionFunctions);
            //}

            JavaScriptUtils = new JavaScriptUtils(null, _engine);

            JavaScriptIndexUtils = new JavaScriptIndexUtils(JavaScriptUtils);
        }

        ~AbstractJavaScriptIndex()
        {
            _definitions.Dispose();
        }

        private void ProcessFields(Dictionary<string, Dictionary<string, List<JavaScriptMapOperation>>> collectionFunctions)
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

        private void ProcessReduce()
        {
            var reduceObjJint = _definitionsJint.GetProperty(ReduceProperty)?.Value;
            if (reduceObjJint != null && reduceObjJint.IsObject())
            {
                var reduceAsObjJint = reduceObjJint.AsObject();
                var groupByKeyJint = reduceAsObjJint.GetProperty(KeyProperty).Value.As<ScriptFunctionInstance>();
                var reduceJint = reduceAsObjJint.GetProperty(AggregateByProperty).Value.As<ScriptFunctionInstance>();

                var reduceObj = _definitions.GetProperty(ReduceProperty);
                if (!reduceObj.IsUndefined && reduceObj.IsObject)
                {
                    var groupByKey = reduceObj.GetProperty(KeyProperty).As<V8Function>();
                    var reduce = reduceObj.GetProperty(AggregateByProperty).As<V8Function>();
                    ReduceOperation = new JavaScriptReduceOperation(reduceJint, groupByKeyJint, JavaScriptIndexUtils, reduce, groupByKey, JavaScriptIndexUtils) { ReduceString = Definition.Reduce };
                    GroupByFields = ReduceOperation.GetReduceFieldsNames();
                    Reduce = ReduceOperation.IndexingFunction;
                }
            }
        }

        protected abstract void ProcessMaps(List<string> mapList, List<MapMetadata> mapReferencedCollections, out Dictionary<string, Dictionary<string, List<JavaScriptMapOperation>>> collectionFunctions);

        protected virtual void OnInitializeEngine()
        {
            _engineJint.SetValue("load", new JintClrFunctionInstance(_engineJint, "load", LoadDocumentJint));
            _engineJint.SetValue("cmpxchg", new JintClrFunctionInstance(_engineJint, "cmpxchg", LoadCompareExchangeValueJint));
            _engineJint.SetValue("tryConvertToNumber", new JintClrFunctionInstance(_engineJint, "tryConvertToNumber", TryConvertToNumberJint));
            _engineJint.SetValue("recurse", new JintClrFunctionInstance(_engineJint, "recurse", RecurseJint));

            _engine.GlobalObject.SetProperty("load", new ClrFunctionInstance(LoadDocument));
            _engine.GlobalObject.SetProperty("cmpxchg", new ClrFunctionInstance(LoadCompareExchangeValue));
            _engine.GlobalObject.SetProperty("tryConvertToNumber", new ClrFunctionInstance(TryConvertToNumber));
            _engine.GlobalObject.SetProperty("recurse", new ClrFunctionInstance(Recurse));
        }

        private List<MapMetadata> InitializeEngine(IndexDefinition definition, List<string> maps, string mapCode)
        {
            OnInitializeEngine();

            _engineJint.ExecuteWithReset(Code);
            _engineJint.ExecuteWithReset(mapCode);


            _engine.ExecuteWithReset(Code, "Code");
            _engine.ExecuteWithReset(MapCode, "MapCode");

            var sb = new StringBuilder();
            if (definition.AdditionalSources != null)
            {
                foreach (var kvpScript in definition.AdditionalSources)
                {
                    var script = kvpScript.Value;
                    _engineJint.ExecuteWithReset(script);
                    _engine.ExecuteWithReset(script, $"additionalSource[{kvpScript.Key}]");
                    sb.Append(Environment.NewLine);
                    sb.AppendLine(script);
                }
            }

            var additionalSources = sb.ToString();
            var mapReferencedCollections = new List<MapMetadata>();
            foreach (var map in maps)
            {
                _engineJint.ExecuteWithReset(map);
                _engine.ExecuteWithReset(map, "map");
                var result = CollectReferencedCollections(map, additionalSources);
                mapReferencedCollections.Add(result);
            }

            if (definition.Reduce != null)
            {
                _engineJint.ExecuteWithReset(definition.Reduce);
                _engine.ExecuteWithReset(definition.Reduce, "reduce");
            }

            return mapReferencedCollections;
        }


        private JsValue RecurseJint(JsValue self, JsValue[] args)
        {
            Debug.Assert(false); // this code is not to be run
            return JsValue.Null;
            /*if (args.Length != 2)
            {
                throw new ArgumentException("The recurse(item, func) method expects two arguments, but got: " + args.Length);
            }

            var item = args[0];
            var func = args[1] as ScriptFunctionInstance;

            if (func == null)
                throw new ArgumentException("The second argument in recurse(item, func) must be an arrow function.");

            return new RecursiveJsFunction(_engineJint, item, func).Execute();*/
        }

        private JsValue TryConvertToNumberJint(JsValue self, JsValue[] args)
        {
            Debug.Assert(false); // this code is not to be run
            return JsValue.Null;
            /*if (args.Length != 1)
            {
                throw new ArgumentException("The tryConvertToNumber(value) method expects one argument, but got: " + args.Length);
            }

            var value = args[0];

            if (value.IsNull() || value.IsUndefined())
                return DynamicJsNull.ImplicitNull;

            if (value.IsNumber)
                return value;

            if (value.IsString)
            {
                var valueAsString = value.AsString;
                if (double.TryParse(valueAsString, out var valueAsDbl))
                    return valueAsDbl;
            }

            return DynamicJsNull.ImplicitNull;*/
        }

        private JsValue LoadDocumentJint(JsValue self, JsValue[] args)
        {
            Debug.Assert(false); // this code is not to be run
            return JsValue.Null;
            /*if (args.Length != 2)
            {
                throw new ArgumentException("The load(id, collection) method expects two arguments, but got: " + args.Length);
            }

            if (args[0].IsNull() || args[0].IsUndefined())
                return DynamicJsNull.ImplicitNull;

            if (args[0].IsString == false ||
                args[1].IsString == false)
            {
                throw new ArgumentException($"The load(id, collection) method expects two string arguments, but got: load({args[0]}, {args[1]})");
            }

            object doc = CurrentIndexingScope.Current.LoadDocument(null, args[0].AsString, args[1].AsString);
            if (JavaScriptIndexUtils.GetValue(_engineJint, doc, out var item))
                return item;

            return DynamicJsNull.ImplicitNull;*/
        }

        private JsValue LoadCompareExchangeValueJint(JsValue self, JsValue[] args)
        {
            Debug.Assert(false); // this code is not to be run
            return JsValue.Null;
            /*if (args.Length != 1)
                throw new ArgumentException("The cmpxchg(key) method expects one argument, but got: " + args.Length);

            var keyArgument = args[0];
            if (keyArgument.IsNull() || keyArgument.IsUndefined())
                return DynamicJsNull.ImplicitNull;

            if (keyArgument.IsString)
            {
                object value = CurrentIndexingScope.Current.LoadCompareExchangeValue(null, keyArgument.AsString);
                return ConvertToJsValue(value);
            }
            else if (keyArgument.IsArray())
            {
                var keys = keyArgument.AsArray();
                if (keys.Length == 0)
                    return DynamicJsNull.ImplicitNull;

                var values = _engineJint.Array.Construct(keys.Length);
                var arrayArgs = new JsValue[1];
                for (uint i = 0; i < keys.Length; i++)
                {
                    var key = keys[i];
                    if (key.IsString == false)
                        ThrowInvalidType(key, Types.String);

                    object value = CurrentIndexingScope.Current.LoadCompareExchangeValue(null, key.AsString);
                    arrayArgs[0] = ConvertToJsValue(value);

                    _engineJint.Array.PrototypeObject.Push(values, args);
                }

                return values;
            }
            else
            {
                throw new InvalidOperationException($"Argument '{keyArgument}' was of type '{keyArgument.Type}', but either string or array of strings was expected.");
            }

            JsValue ConvertToJsValue(object value)
            {
                switch (value)
                {
                    case null:
                        return DynamicJsNull.ImplicitNull;

                    case DynamicNullObject dno:
                        return dno.IsExplicitNull ? DynamicJsNull.ExplicitNull : DynamicJsNull.ImplicitNull;

                    case DynamicBlittableJson dbj:
                        return new BlittableObjectInstance(_engineJint, null, dbj.BlittableJson, id: null, lastModified: null, changeVector: null);

                    default:
                        return JavaScriptUtils.TranslateToJs(_engineJint, context: null, value);
                }
            }

            static void ThrowInvalidType(JsValue value, Types expectedType)
            {
                throw new InvalidOperationException($"Argument '{value}' was of type '{value.Type}', but '{expectedType}' was expected.");
            }*/
        }


        private InternalHandle Recurse(V8EngineEx engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            if (args.Length != 2)
            {
                throw new ArgumentException("The recurse(item, func) method expects two arguments, but got: " + args.Length);
            }

            var item = args[0];
            var func = args[1].Object as V8Function;

            if (func == null)
                throw new ArgumentException("The second argument in recurse(item, func) must be an arrow function.");

            return new RecursiveJsFunction(_engine, item, func).Execute();
        }

        private static InternalHandle TryConvertToNumber(V8EngineEx engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            if (args.Length != 1)
            {
                throw new ArgumentException("The tryConvertToNumber(value) method expects one argument, but got: " + args.Length);
            }

            InternalHandle jsRes;
            var value = args[0];

            if (value.IsNull || value.IsUndefined)
                return jsRes.Set(DynamicJsNull.ImplicitNull._);

            if (value.IsNumber)
                return value;

            if (value.IsString)
            {
                var valueAsString = value.AsString;
                if (Double.TryParse(valueAsString, out var valueAsDbl))
                    return valueAsDbl;
            }

            return jsRes.Set(DynamicJsNull.ImplicitNull._);
        }

        private static InternalHandle LoadDocument(V8EngineEx engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            if (args.Length != 2)
            {
                throw new ArgumentException("The load(id, collection) method expects two arguments, but got: " + args.Length);
            }

            InternalHandle jsRes;
            if (args[0].IsNull || args[0].IsUndefined)
                return jsRes.Set(DynamicJsNull.ImplicitNull._);

            if (args[0].IsString == false ||
                args[1].IsString == false)
            {
                throw new ArgumentException($"The load(id, collection) method expects two string arguments, but got: load({args[0]}, {args[1]})");
            }

            object doc = CurrentIndexingScope.Current.LoadDocument(null, args[0].AsString, args[1].AsString);
            if (JavaScriptIndexUtils.GetValue(doc, out InternalHandle jsItem))
                return jsItem;

            return jsRes.Set(DynamicJsNull.ImplicitNull._);
        }

        private static InternalHandle LoadCompareExchangeValue(V8EngineEx engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            if (args.Length != 1)
                throw new ArgumentException("The cmpxchg(key) method expects one argument, but got: " + args.Length);

            InternalHandle jsRes;
            var keyArgument = args[0];
            if (keyArgument.IsNull || keyArgument.IsUndefined)
                return jsRes.Set(DynamicJsNull.ImplicitNull._);

            if (keyArgument.IsString)
            {
                object value = CurrentIndexingScope.Current.LoadCompareExchangeValue(null, keyArgument.AsString);
                return ConvertToJsValue(value);
            }
            else if (keyArgument.IsArray)
            {
                var keys = keyArgument.Object;
                int arrayLength =  keys.ArrayLength;
                if (arrayLength == 0)
                    return jsRes.Set(DynamicJsNull.ImplicitNull._);

                var jsItems = new InternalHandle[arrayLength];
                for (int i = 0; i < arrayLength; i++)
                {
                    using (var key = keys.GetProperty(i)) 
                    {
                        if (key.IsString == false)
                            ThrowInvalidType(key, JSValueType.String);

                        object value = CurrentIndexingScope.Current.LoadCompareExchangeValue(null, key.AsString);
                        jsItems[i] = ConvertToJsValue(value);
                    }
                }

                return _engine.CreateArrayWithDisposal(jsItems);
            }
            else
            {
                throw new InvalidOperationException($"Argument '{keyArgument}' was of type '{keyArgument.ValueType}', but either string or array of strings was expected.");
            }

            InternalHandle ConvertToJsValue(object value)
            {
                InternalHandle jsRes;
                switch (value)
                {
                    case null:
                        return jsRes.Set(DynamicJsNull.ImplicitNull._);

                    case DynamicNullObject dno: {
                        var dynamicNull = dno.IsExplicitNull ? DynamicJsNull.ExplicitNull : DynamicJsNull.ImplicitNull;
                        return jsRes.Set(dynamicNull._);
                    }

                    case DynamicBlittableJson dbj: {
                        BlittableObjectInstance boi = new BlittableObjectInstance(JavaScriptUtils, null, dbj.BlittableJson, id: null, lastModified: null, changeVector: null);
                        return jsRes.Set(boi.CreateObjectBinder()._);
                    }

                    default:
                        return JavaScriptUtils.TranslateToJs(context: null, value);
                }
            }

            static void ThrowInvalidType(InternalHandle jsValue, JSValueType expectedType)
            {
                throw new InvalidOperationException($"Argument '{jsValue}' was of type '{jsValue.ValueType}', but '{expectedType}' was expected.");
            }
        }

        private InternalHandle GetDefinitions()
        {
            var definitions = _engine.GlobalObject.GetProperty(GlobalDefinitions);

            if (definitions.IsNull || definitions.IsUndefined || definitions.IsObject == false)
                ThrowIndexCreationException($"is missing its '{GlobalDefinitions}' global variable, are you modifying it in your script?");

            if (definitions.GetProperty(MapsProperty).IsUndefined)
                ThrowIndexCreationException($"is missing its '{MapsProperty}' property, are you modifying it in your script?");

            return definitions;
        }

        private ObjectInstance GetDefinitionsJint()
        {
            var definitionsObj = _engineJint.GetValue(GlobalDefinitions);

            if (definitionsObj.IsNull() || definitionsObj.IsUndefined() || definitionsObj.IsObject() == false)
                ThrowIndexCreationException($"is missing its '{GlobalDefinitions}' global variable, are you modifying it in your script?");

            var definitions = definitionsObj.AsObject();
            if (definitions.HasProperty(MapsProperty) == false)
                ThrowIndexCreationException("is missing its 'globalDefinition.maps' property, are you modifying it in your script?");

            return definitions;
        }

        private static readonly ParserOptions DefaultParserOptions = new ParserOptions();

        private MapMetadata CollectReferencedCollections(string code, string additionalSources)
        {
            var javascriptParser = new JavaScriptParser(code, DefaultParserOptions);
            var program = javascriptParser.ParseScript();
            var loadVisitor = new EsprimaReferencedCollectionVisitor();
            if (string.IsNullOrEmpty(additionalSources) == false)
                loadVisitor.Visit(new JavaScriptParser(additionalSources, DefaultParserOptions).ParseScript());

            loadVisitor.Visit(program);
            return new MapMetadata
            {
                ReferencedCollections = loadVisitor.ReferencedCollection,
                HasCompareExchangeReferences = loadVisitor.HasCompareExchangeReferences
            };
        }
        private List<string> GetMappingFunctions(Action<List<string>> modifyMappingFunctions)
        {
            if (Definition.Maps == null || Definition.Maps.Count == 0)
                ThrowIndexCreationException("does not contain any mapping functions to process.");

            var mappingFunctions = Definition.Maps.ToList();
            
            modifyMappingFunctions?.Invoke(mappingFunctions);

            return mappingFunctions;
        }
        protected void ThrowIndexCreationException(string message)
        {
            throw new IndexCreationException($"JavaScript index {Definition.Name} {message}");
        }

        private const string Code = @"
var globalDefinition =
{
    maps: [],
    reduce: null
}
function groupBy(lambda) {
    var reduce = globalDefinition.reduce = { };
    reduce.key = lambda;
    reduce.aggregate = function(reduceFunction){
        reduce.aggregateBy = reduceFunction;
    }
    return reduce;
}
// createSpatialField(wkt: string)
// createSpatialField(lat: number, lng: number)
function createSpatialField() {
    if(arguments.length == 1) {
        return { $spatial: arguments[0] }
}
    return { $spatial: {Lng: arguments[1], Lat: arguments[0]} }
}
function createField(name, value, options) {
    return { $name: name, $value: value, $options: options }
}
function boost(value, boost) {
    return { $value: value, $boost: boost }
}
";

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
    }

}
