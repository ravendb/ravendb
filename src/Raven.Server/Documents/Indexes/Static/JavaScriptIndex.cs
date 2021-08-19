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
using Raven.Server.Documents.Jint.Patch;

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
    globalDefinition.desc = 'Descripton is ok';
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

            _engine.SetGlobalCLRCallBack("getMetadata", MetadataFor); // for backward-compatibility only
            _engine.SetGlobalCLRCallBack("metadataFor", MetadataFor);
            _engine.SetGlobalCLRCallBack("attachmentsFor", AttachmentsFor);
            _engine.SetGlobalCLRCallBack("timeSeriesNamesFor", TimeSeriesNamesFor);
            _engine.SetGlobalCLRCallBack("counterNamesFor", CounterNamesFor);
            _engine.SetGlobalCLRCallBack("loadAttachment", LoadAttachment);
            _engine.SetGlobalCLRCallBack("loadAttachments", LoadAttachments);
            _engine.SetGlobalCLRCallBack("id", GetDocumentId);

        }

        protected override void ProcessMaps(List<string> mapList, List<MapMetadata> mapReferencedCollections, out Dictionary<string, Dictionary<string, List<JavaScriptMapOperation>>> collectionFunctions)
        {
            var mapsArrayJint = _definitionsJint.GetProperty(MapsProperty).Value;
            if (mapsArrayJint.IsNull() || mapsArrayJint.IsUndefined() || mapsArrayJint.IsArray() == false)
                ThrowIndexCreationException($"Jint: doesn't contain any map function or '{GlobalDefinitions}.{Maps}' was modified in the script");

            var mapsJint = mapsArrayJint.AsArray();
            if (mapsJint.Length == 0)
                ThrowIndexCreationException($"Jint: doesn't contain any map functions or '{GlobalDefinitions}.{Maps}' was modified in the script");


            /*var desc3 = InternalHandle.Empty;
            using (var desc2 = _engine.CreateValue("desc"))
            {
                desc3 = new InternalHandle(desc2, true);
            }
            desc3.Dispose();
            using (var desc2 = _engine.CreateValue("desc"))
            {
                desc3.Set(desc2);
            }
            desc3.Dispose();

            var desc1 = InternalHandle.Empty;
            using (var desc = _definitions.GetProperty("desc")) // pure native value
            {
                desc1 = new InternalHandle(desc, true);
            }
            desc1.Dispose();
            using (var desc = _definitions.GetProperty("desc")) // pure native value
            {
                desc1.Set(desc);
            }
            desc1.Dispose();*/

            //var maps1 = InternalHandle.Empty;
            //var map1 = InternalHandle.Empty;
            using (var maps = _definitions.GetProperty(MapsProperty)) // pure native value
            {
                //maps1 = new InternalHandle(maps, true);
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

                    using (var map = maps.GetProperty(i)) // pure native value
                    {
                        //map1.Set(map);
                        if (map.IsNull || map.IsUndefined || map.IsObject == false)
                            ThrowIndexCreationException($"map function #{i} is not a valid object");
                        if (map.HasProperty(CollectionProperty) == false)
                            ThrowIndexCreationException($"map function #{i} is missing a collection name");
                        using (var mapCollectionStr = map.GetProperty(CollectionProperty)) {
                            if (mapCollectionStr.IsStringEx() == false)
                                ThrowIndexCreationException($"map function #{i} collection name isn't a string");
                            var mapCollection = mapCollectionStr.AsString;

                            if (collectionFunctions.TryGetValue(mapCollection, out var subCollectionFunctions) == false)
                                collectionFunctions[mapCollection] = subCollectionFunctions = new Dictionary<string, List<JavaScriptMapOperation>>();

                            if (subCollectionFunctions.TryGetValue(mapCollection, out var list) == false)
                                subCollectionFunctions[mapCollection] = list = new List<JavaScriptMapOperation>();

                            if (map.HasProperty(MethodProperty) == false)
                                ThrowIndexCreationException($"map function #{i} is missing its {MethodProperty} property");

                            var func = map.GetProperty(MethodProperty); // to be saved to map operation
                            {
                                if (func.IsFunction == false)
                                    ThrowIndexCreationException($"map function #{i} {MethodProperty} property isn't a function");

                                JavaScriptMapOperation operation = new JavaScriptMapOperation(JavaScriptIndexUtils, funcInstanceJint, func, Definition.Name, mapList[i]);
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


        private InternalHandle GetDocumentId(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            try {
                var scope = CurrentIndexingScope.Current;
                scope.RegisterJavaScriptUtils(JavaScriptUtils);

                return JavaScriptUtils.GetDocumentId(engine, isConstructCall, self, args);
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.Message, JSValueType.ExecutionError);
            }
        }

        private InternalHandle AttachmentsFor(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            try {
                var scope = CurrentIndexingScope.Current;
                scope.RegisterJavaScriptUtils(JavaScriptUtils);

                return JavaScriptUtils.AttachmentsFor(engine, isConstructCall, self, args);
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.Message, JSValueType.ExecutionError);
            }
        }

        private InternalHandle MetadataFor(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            try {
                var scope = CurrentIndexingScope.Current;
                scope.RegisterJavaScriptUtils(JavaScriptUtils);

                return JavaScriptUtils.GetMetadata(engine, isConstructCall, self, args);
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.Message, JSValueType.ExecutionError);
            }
        }

        private InternalHandle TimeSeriesNamesFor(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            try {
                var scope = CurrentIndexingScope.Current;
                scope.RegisterJavaScriptUtils(JavaScriptUtils);

                return JavaScriptUtils.GetTimeSeriesNamesFor(engine, isConstructCall, self, args);
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.Message, JSValueType.ExecutionError);
            }
        }

        private InternalHandle CounterNamesFor(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            try {
                var scope = CurrentIndexingScope.Current;
                scope.RegisterJavaScriptUtils(JavaScriptUtils);

                return JavaScriptUtils.GetCounterNamesFor(engine, isConstructCall, self, args);
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.Message, JSValueType.ExecutionError);
            }
        }

        private InternalHandle LoadAttachment(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            try {
                var scope = CurrentIndexingScope.Current;
                scope.RegisterJavaScriptUtils(JavaScriptUtils);

                return JavaScriptUtils.LoadAttachment(engine, isConstructCall, self, args);
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.Message, JSValueType.ExecutionError);
            }
        }

        private InternalHandle LoadAttachments(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            try {
                var scope = CurrentIndexingScope.Current;
                scope.RegisterJavaScriptUtils(JavaScriptUtils);

                return JavaScriptUtils.LoadAttachments(engine, isConstructCall, self, args);
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.Message, JSValueType.ExecutionError);
            }
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


        //private JintNullPropagationReferenceResolver _resolverJint;
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

            //_resolverJint = new JintNullPropagationReferenceResolver(); //JintPreventResolvingTasksReferenceResolver();
            _engineJint = new Engine(); /*options =>
            {
                options
                    .LimitRecursion(64)
                    .SetReferencesResolver(_resolverJint)
                    .MaxStatements(indexConfiguration.MaxStepsForScript)
                    .Strict(configuration.Patching.StrictMode)
                    //.AddObjectConverter(new JintGuidConverter())
                    //.AddObjectConverter(new JintStringConverter())
                    //.AddObjectConverter(new JintEnumConverter())
                    //.AddObjectConverter(new JintDateTimeConverter())
                    //.AddObjectConverter(new JintTimeSpanConverter())
                    .LocalTimeZone(TimeZoneInfo.Utc);
            });*/

            // we create the engine instance directly instead of using SingleRun
            // because the index is single threaded and long lived
            _engine = new V8EngineEx();

            JavaScriptUtils = new JavaScriptUtils(null, _engine);

            JavaScriptIndexUtils = new JavaScriptIndexUtils(JavaScriptUtils, _engineJint);

            string strictModeFlag = configuration.Patching.StrictMode ? "--use_strict" : "--no-use_strict";
            string[] optionsCmd = {strictModeFlag}; // TODO construct from options
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
        }

        ~AbstractJavaScriptIndex()
        {
            _engine.Dispose();
            _definitions.Dispose(); // pure native value
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

                using (var reduceObj = _definitions.GetProperty(ReduceProperty)) // pure native value
                {
                    if (!reduceObj.IsUndefined && reduceObj.IsObject)
                    {
                        var groupByKey = reduceObj.GetProperty(KeyProperty); // to be saved to reduce operation
                        var reduce = reduceObj.GetProperty(AggregateByProperty); // to be saved to reduce operation
                        ReduceOperation = new JavaScriptReduceOperation(reduceJint, groupByKeyJint, _engineJint, reduce, groupByKey, JavaScriptIndexUtils) { ReduceString = Definition.Reduce };
                        GroupByFields = ReduceOperation.GetReduceFieldsNames();
                        Reduce = ReduceOperation.IndexingFunction;
                    }
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

            _engine.SetGlobalCLRCallBack("load", LoadDocument);
            _engine.SetGlobalCLRCallBack("cmpxchg", LoadCompareExchangeValue);
            _engine.SetGlobalCLRCallBack("tryConvertToNumber", TryConvertToNumber);
            _engine.SetGlobalCLRCallBack("recurse", Recurse);
        }

        private List<MapMetadata> InitializeEngine(IndexDefinition definition, List<string> maps, string mapCode)
        {
            OnInitializeEngine();

            _engine.ExecuteWithReset(Code, "Code");
            _engine.ExecuteWithReset(MapCode, "MapCode");

            _engineJint.ExecuteWithReset(Code);
            _engineJint.ExecuteWithReset(mapCode);

            var sb = new StringBuilder();
            if (definition.AdditionalSources != null)
            {
                foreach (var kvpScript in definition.AdditionalSources)
                {
                    var script = kvpScript.Value;
                    _engine.ExecuteWithReset(script, $"additionalSource[{kvpScript.Key}]");
                    _engineJint.ExecuteWithReset(script);
                    sb.Append(Environment.NewLine);
                    sb.AppendLine(script);
                }
            }

            var additionalSources = sb.ToString();
            var mapReferencedCollections = new List<MapMetadata>();
            foreach (var map in maps)
            {
                _engine.ExecuteWithReset(map, "map");
                _engineJint.ExecuteWithReset(map);
                var result = CollectReferencedCollections(map, additionalSources);
                mapReferencedCollections.Add(result);
            }

            if (definition.Reduce != null)
            {
                _engine.ExecuteWithReset(definition.Reduce, "reduce");
                _engineJint.ExecuteWithReset(definition.Reduce);
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

            if (value.IsNumber())
                return value;

            if (value.IsString())
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

            if (args[0].IsStringEx() == false ||
                args[1].IsStringEx() == false)
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

            if (keyArgument.IsStringEx())
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
                    if (key.IsStringEx() == false)
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


        private InternalHandle Recurse(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            try {
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
            catch (Exception e) 
            {
                return engine.CreateError(e.Message, JSValueType.ExecutionError);
            }
        }


        //public InternalHandle jsTest;

        private InternalHandle TryConvertToNumber(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            try {
                if (args.Length != 1)
                    throw new ArgumentException("The tryConvertToNumber(value) method expects one argument, but got: " + args.Length);

                InternalHandle value = args[0];
                InternalHandle jsRes = InternalHandle.Empty;

                /*jsTest = new InternalHandle(value, true);
                var v1 = InternalHandle.Empty;
                var v2 = InternalHandle.Empty;
                var v3 = InternalHandle.Empty;
                v1.Set(value);
                v2.Set(value);
                using (v1) {
                    v3.Set(v1);
                }
                v2.Dispose();
                v3.Dispose();*/

                if (value.IsNull || value.IsUndefined)
                    return DynamicJsNull.ImplicitNull._;

                if (value.IsNumberOrIntEx())
                    return jsRes.Set(value);

                if (value.IsStringEx())
                {
                    if (Double.TryParse(value.AsString, out var valueAsDbl)) {
                        return engine.CreateValue(valueAsDbl);
                    }
                }

                return DynamicJsNull.ImplicitNull._;
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.Message, JSValueType.ExecutionError);
            }
        }

        private InternalHandle LoadDocument(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            try {
                if (args.Length != 2)
                {
                    throw new ArgumentException("The load(id, collection) method expects two arguments, but got: " + args.Length);
                }

                InternalHandle jsRes = InternalHandle.Empty;
                if (args[0].IsNull || args[0].IsUndefined)
                    return DynamicJsNull.ImplicitNull._;

                if (args[0].IsStringEx() == false ||
                    args[1].IsStringEx() == false)
                {
                    throw new ArgumentException($"The load(id, collection) method expects two string arguments, but got: load({args[0]}, {args[1]})");
                }

                object doc = CurrentIndexingScope.Current.LoadDocument(null, args[0].AsString, args[1].AsString);
                if (JavaScriptIndexUtils.GetValue(doc, out InternalHandle jsItem))
                    return jsItem;

                return DynamicJsNull.ImplicitNull._;
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.Message, JSValueType.ExecutionError);
            }
        }

        private InternalHandle LoadCompareExchangeValue(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
        {
            try {
                if (args.Length != 1)
                    throw new ArgumentException("The cmpxchg(key) method expects one argument, but got: " + args.Length);

                InternalHandle jsRes = InternalHandle.Empty;
                var keyArgument = args[0];
                if (keyArgument.IsNull || keyArgument.IsUndefined)
                    return DynamicJsNull.ImplicitNull._;

                if (keyArgument.IsStringEx())
                {
                    object value = CurrentIndexingScope.Current.LoadCompareExchangeValue(null, keyArgument.AsString);
                    return ConvertToJsValue(value);
                }
                else if (keyArgument.IsArray)
                {
                    int arrayLength =  keyArgument.ArrayLength;
                    if (arrayLength == 0)
                        return DynamicJsNull.ImplicitNull._;

                    var jsItems = new InternalHandle[arrayLength];
                    for (int i = 0; i < arrayLength; i++)
                    {
                        using (var key = keyArgument.GetProperty(i)) 
                        {
                            if (key.IsStringEx() == false)
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
            }
            catch (Exception e) 
            {
                return engine.CreateError(e.Message, JSValueType.ExecutionError);
            }

            InternalHandle ConvertToJsValue(object value)
            {
                InternalHandle jsRes = InternalHandle.Empty;
                switch (value)
                {
                    case null:
                        return DynamicJsNull.ImplicitNull._;

                    case DynamicNullObject dno: {
                        var dynamicNull = dno.IsExplicitNull ? DynamicJsNull.ExplicitNull : DynamicJsNull.ImplicitNull;
                        return dynamicNull._;
                    }

                    case DynamicBlittableJson dbj: {
                        BlittableObjectInstance boi = new BlittableObjectInstance(JavaScriptUtils, null, dbj.BlittableJson, id: null, lastModified: null, changeVector: null);
                        return boi.CreateObjectBinder();
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
var process = {
    env: {
        EXEC_ENV: 'RavenDB'
    }
}
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
