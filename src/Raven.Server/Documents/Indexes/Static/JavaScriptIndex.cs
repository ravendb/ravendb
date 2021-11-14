using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.Configuration;
using Raven.Client.ServerWide.JavaScript;

using Jint; // actually we need Esprima for analyzing groupings, but for now we use it in the old way by means of Jint (having the outdated Esprima parser version not supporting some new features like optional chaining operator '?.')
using Jint.Native;
using Jint.Native.Function;
using Jint.Native.Object;
using Jint.Runtime;
using Esprima; // TODO to switch to the latest version Esprima directly or maybe even better to eliminate the need for it by implementing groupBy as CLR callback (why not?), but this is not critical thanks to the little trick descibed in JintEngineExForV8::ProcessJintStub

using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Indexes.Static.Utils;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Documents.Patch.V8;
using Raven.Server.Documents.Indexes.Static.TimeSeries;
using Raven.Server.Documents.Indexes.Static.Counters;
using JintEngineExForV8 = Raven.Server.Extensions.V8.JintEngineExForV8;

namespace Raven.Server.Documents.Indexes.Static
{
    public sealed partial class JavaScriptIndex : AbstractJavaScriptIndex
    {
        public const string NoTracking = "noTracking";

        public const string Load = "load";

        public const string CmpXchg = "cmpxchg";
        
        public JavaScriptIndex(IndexDefinition definition, RavenConfiguration configuration, long indexVersion)
            : base(definition, configuration, modifyMappingFunctions: null, JavaScriptIndex.GetMapCode(), indexVersion)
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

            EngineHandle.SetGlobalClrCallBack("getMetadata", (MetadataForJint, MetadataForV8)); // for backward-compatibility only
            EngineHandle.SetGlobalClrCallBack("metadataFor", (MetadataForJint, MetadataForV8));
            EngineHandle.SetGlobalClrCallBack("attachmentsFor", (AttachmentsForJint, AttachmentsForV8));
            EngineHandle.SetGlobalClrCallBack("timeSeriesNamesFor", (TimeSeriesNamesForJint, TimeSeriesNamesForV8));
            EngineHandle.SetGlobalClrCallBack("counterNamesFor", (CounterNamesForJint, CounterNamesForV8));
            EngineHandle.SetGlobalClrCallBack("loadAttachment", (LoadAttachmentJint, LoadAttachmentV8));
            EngineHandle.SetGlobalClrCallBack("loadAttachments", (LoadAttachmentsJint, LoadAttachmentsV8));
            EngineHandle.SetGlobalClrCallBack("id", (GetDocumentIdJint, GetDocumentIdV8));

        }

        protected override void ProcessMaps(List<string> mapList, List<MapMetadata> mapReferencedCollections, out Dictionary<string, Dictionary<string, List<JavaScriptMapOperation>>> collectionFunctions)
        {
            using (var maps = _definitions.GetProperty(MapsProperty)) 
            {
                if (maps.IsNull || maps.IsUndefined || maps.IsArray == false)
                    ThrowIndexCreationException($"doesn't contain any map function or '{GlobalDefinitions}.{Maps}' was modified in the script");

                var mapCount = maps.ArrayLength;
                if (mapCount == 0)
                    ThrowIndexCreationException($"doesn't contain any map functions or '{GlobalDefinitions}.{Maps}' was modified in the script");

                var mapsArrayForParsingJint = _definitionsForParsingJint.GetProperty(MapsProperty).Value;
                if (mapsArrayForParsingJint.IsNull() || mapsArrayForParsingJint.IsUndefined() || mapsArrayForParsingJint.IsArray() == false)
                    ThrowIndexCreationException($"Jint doesn't contain any map function");

                var mapsJint = mapsArrayForParsingJint.AsArray();

                collectionFunctions = new Dictionary<string, Dictionary<string, List<JavaScriptMapOperation>>>();
                for (int i = 0; i < mapCount; i++)
                {
                    if (i >= mapsJint.Length)
                        ThrowIndexCreationException($"JavaScript: Jint: maps length is less or equal then #{i}");
                    var mapForParsingJint = mapsJint.Get(i.ToString());
                    if (mapForParsingJint.IsUndefined())
                        ThrowIndexCreationException($"JavaScript: Jint: map #{i} is null");
                    var mapObjForParsingJint = mapForParsingJint.AsObject();                        
                    if (mapObjForParsingJint == null)
                        ThrowIndexCreationException($"JavaScript: Jint: map #{i} is not object");
                    if (mapObjForParsingJint.HasProperty(MethodProperty) == false)
                        ThrowIndexCreationException($"JavaScript: Jint: map function #{i} is missing its {MethodProperty} property");
                    var funcForParsingJint = mapObjForParsingJint.Get(MethodProperty).As<FunctionInstance>();
                    if (funcForParsingJint == null)
                        ThrowIndexCreationException($"JavaScript: Jint: map function #{i} {MethodProperty} property isn't a 'FunctionInstance'");

                    using (var map = maps.GetProperty(i)) 
                    {
                        if (map.IsNull || map.IsUndefined || map.IsObject == false)
                            ThrowIndexCreationException($"map function #{i} is not a valid object");
                        if (map.HasProperty(CollectionProperty) == false)
                            ThrowIndexCreationException($"map function #{i} is missing a collection name");
                        using (var mapCollectionStr = map.GetProperty(CollectionProperty))
                        {
                            if (mapCollectionStr.IsStringEx == false)
                                ThrowIndexCreationException($"map function #{i} collection name isn't a string");
                            var mapCollection = mapCollectionStr.AsString;

                            if (collectionFunctions.TryGetValue(mapCollection, out var subCollectionFunctions) == false)
                                collectionFunctions[mapCollection] = subCollectionFunctions = new Dictionary<string, List<JavaScriptMapOperation>>();

                            if (subCollectionFunctions.TryGetValue(mapCollection, out var list) == false)
                                subCollectionFunctions[mapCollection] = list = new List<JavaScriptMapOperation>();

                            if (map.HasProperty(MethodProperty) == false)
                                ThrowIndexCreationException($"map function #{i} is missing its {MethodProperty} property");

                            using (var func = map.GetProperty(MethodProperty))
                            {
                                if (func.IsFunction == false)
                                    ThrowIndexCreationException($"map function #{i} {MethodProperty} property isn't a function");

                                JavaScriptMapOperation operation = new JavaScriptMapOperation(this, JsIndexUtils, funcForParsingJint, func, Definition.Name, mapList[i]);
                                if (mapObjForParsingJint != null && mapObjForParsingJint.HasOwnProperty(MoreArgsProperty))
                                {
                                    var moreArgsObjJint = mapObjForParsingJint.Get(MoreArgsProperty);
                                    if (moreArgsObjJint.IsArray())
                                    {
                                        var arrayJint = moreArgsObjJint.AsArray();
                                        if (arrayJint.Length > 0)
                                        {
                                            operation.MoreArguments = arrayJint;
                                        }
                                    }
                                }

                                operation.Analyze((Engine)_engineForParsing);
                                
                                var referencedCollections = mapReferencedCollections[i].ReferencedCollections;
                                if (referencedCollections.Count > 0)
                                {
                                    if (ReferencedCollections.TryGetValue(mapCollection, out var collectionNames) == false)
                                    {
                                        collectionNames = new HashSet<CollectionName>();
                                        ReferencedCollections.Add(mapCollection, collectionNames);
                                    }

                                    collectionNames.UnionWith(mapReferencedCollections[i].ReferencedCollections);
                                    collectionNames.UnionWith(referencedCollections);
                                }
                                
                                if (mapReferencedCollections[i].HasCompareExchangeReferences)
                                    CollectionsWithCompareExchangeReferences.Add(mapCollection);

                                list.Add(operation);
                            }
                        }
                    }
                }
            }
        }
    }

    public abstract partial class AbstractJavaScriptIndex : AbstractJavaScriptIndexBase
    {
        protected JavaScriptEngineType _jsEngineType;
        
        public readonly IJavaScriptUtils JsUtils;
        public readonly JavaScriptIndexUtils JsIndexUtils;

        protected IJavaScriptEngineForParsing _engineForParsing; // is used for maps static analysis, but not for running
        protected ObjectInstance _definitionsForParsingJint;

        public IJsEngineHandle EngineHandle;
        protected JsHandle _definitions;
        private readonly long _indexVersion;
        
        public static AbstractJavaScriptIndex Create(IndexDefinition definition, RavenConfiguration configuration, long indexVersion)
        {
            switch (definition.SourceType)
            {
                case IndexSourceType.Documents:
                    return new JavaScriptIndex(definition, configuration, indexVersion);
                case IndexSourceType.TimeSeries:
                    return new TimeSeriesJavaScriptIndex(definition, configuration, indexVersion);
                case IndexSourceType.Counters:
                    return new CountersJavaScriptIndex(definition, configuration, indexVersion);
                default:
                    throw new NotSupportedException($"Not supported source type '{definition.SourceType}'.");
            }
        }

        protected AbstractJavaScriptIndex(IndexDefinition definition, RavenConfiguration configuration, Action<List<string>> modifyMappingFunctions, string mapCode, long indexVersion)
            : base(definition, configuration, mapCode)
        {
            _jsEngineType = JsOptions.EngineType;

            // we create the engine instance directly instead of using SingleRun
            // because the index is single threaded and long lived
            InitializeEngineSpecific();

            JsUtils = JavaScriptUtilsBase.Create(null, EngineHandle);
            JsIndexUtils = new JavaScriptIndexUtils(JsUtils, _engineForParsing);

            _indexVersion = indexVersion;

            InitializeEngineSpecific2();

            lock (EngineHandle)
            {
                switch (_jsEngineType)
                {
                    case JavaScriptEngineType.Jint:
                        InitializeLockedJint();
                        break;
                    case JavaScriptEngineType.V8:
                        InitializeLockedV8();
                        break;
                    default:
                        throw new NotSupportedException($"Not supported JS engine kind '{_jsEngineType}'.");
                }

                using (EngineHandle.DisableConstraints())
                using (_engineForParsing.DisableConstraints())
                {
                    var maps = GetMappingFunctions(modifyMappingFunctions);

                    var mapReferencedCollections = InitializeEngine(maps);

                    _definitionsForParsingJint = GetDefinitionsForParsing();
                    _definitions = GetDefinitions();

                    ProcessMaps(maps, mapReferencedCollections, out var collectionFunctions);

                    ProcessReduce();

                    ProcessFields(collectionFunctions);
                }
            }
        }

        ~AbstractJavaScriptIndex()
        {
            _definitions.Dispose();
            
            switch (_jsEngineType)
            {
                case JavaScriptEngineType.Jint:
                    DisposeJint();
                    break;
                case JavaScriptEngineType.V8:
                    DisposeV8();
                    break;
                default:
                    throw new NotSupportedException($"Not supported JS engine kind '{_jsEngineType}'.");
            }
        }

        protected void InitializeEngineSpecific()
        {
            switch (_jsEngineType)
            {
                case JavaScriptEngineType.Jint:
                    InitializeJint();
                    break;
                case JavaScriptEngineType.V8:
                    InitializeV8();
                    break;
                default:
                    throw new NotSupportedException($"Not supported JS engine kind '{_jsEngineType}'.");
            }
        }

        protected void InitializeEngineSpecific2()
        {
            switch (_jsEngineType)
            {
                case JavaScriptEngineType.Jint:
                    InitializeJint2();
                    break;
                case JavaScriptEngineType.V8:
                    InitializeV82();
                    break;
                default:
                    throw new NotSupportedException($"Not supported JS engine kind '{_jsEngineType}'.");
            }
        }

        private void ProcessReduce()
        {
            using (var reduceObj = _definitions.GetProperty(ReduceProperty)) 
            {
                if (!reduceObj.IsUndefined && reduceObj.IsObject)
                {
                    var reduceObjForParsingJint = _definitionsForParsingJint.GetProperty(ReduceProperty)?.Value;
                    if (reduceObjForParsingJint != null && reduceObjForParsingJint.IsObject())
                    {
                        var reduceAsObjForParsingJint = reduceObjForParsingJint?.AsObject();
                        var groupByKeyForParsingJint = reduceAsObjForParsingJint?.GetProperty(KeyProperty).Value.As<ScriptFunctionInstance>();
                        if (groupByKeyForParsingJint == null)
                        {
                            throw new ArgumentException("Failed to get reduce key object");
                        }

                        using (var groupByKey = reduceObj.GetProperty(KeyProperty))
                        using (var reduce = reduceObj.GetProperty(AggregateByProperty))
                            ReduceOperation = new JavaScriptReduceOperation(this, JsIndexUtils, groupByKeyForParsingJint, _engineForParsing, reduce, groupByKey, _indexVersion) { ReduceString = Definition.Reduce };
                        GroupByFields = ReduceOperation.GetReduceFieldsNames();
                        Reduce = ReduceOperation.IndexingFunction;
                    }
                    else
                        throw new ArgumentException("Failed to get the reduce object: ");
                }
            }
        }

        protected abstract void ProcessMaps(List<string> mapList, List<MapMetadata> mapReferencedCollections, out Dictionary<string, Dictionary<string, List<JavaScriptMapOperation>>> collectionFunctions);

        protected virtual void OnInitializeEngine()
        {
            var loadFunc = EngineHandle.CreateClrCallBack(JavaScriptIndex.Load, (LoadDocumentJint, LoadDocumentV8));

            var noTrackingObject = EngineHandle.CreateObject();
            noTrackingObject.FastAddProperty(JavaScriptIndex.Load, loadFunc.Clone(), false, false, false);
            EngineHandle.SetGlobalProperty(JavaScriptIndex.NoTracking, noTrackingObject);

            EngineHandle.SetGlobalProperty(JavaScriptIndex.Load, loadFunc);
            EngineHandle.SetGlobalClrCallBack(JavaScriptIndex.CmpXchg, (LoadCompareExchangeValueJint, LoadCompareExchangeValueV8));
            EngineHandle.SetGlobalClrCallBack("tryConvertToNumber", (TryConvertToNumberJint, TryConvertToNumberV8));
            EngineHandle.SetGlobalClrCallBack("recurse", (RecurseJint, RecurseV8));
        }

        private List<MapMetadata> InitializeEngine(List<string> maps)
        {
            OnInitializeEngine();

            EngineHandle.ExecuteWithReset(Code, "Code");
            EngineHandle.ExecuteWithReset(MapCode, "MapCode");

            if (_jsEngineType == JavaScriptEngineType.V8)
            {
                _engineForParsing.ExecuteWithReset(Code);
                _engineForParsing.ExecuteWithReset(MapCode);
            }

            var sb = new StringBuilder();
            if (Definition.AdditionalSources != null)
            {
                foreach (var kvpScript in Definition.AdditionalSources)
                {
                    var script = kvpScript.Value;
                    EngineHandle.ExecuteWithReset(script, $"./{Definition.Name}/additionalSource/{kvpScript.Key}");
                    if (_jsEngineType == JavaScriptEngineType.V8)
                        _engineForParsing.ExecuteWithReset(script);
                    sb.Append(Environment.NewLine);
                    sb.AppendLine(script);
                }
            }

            var additionalSources = sb.ToString();
            var mapReferencedCollections = new List<MapMetadata>();
            foreach (var map in maps)
            {
                EngineHandle.ExecuteWithReset(map, "map");
                if (_jsEngineType == JavaScriptEngineType.V8)
                    _engineForParsing.ExecuteWithReset(map);
                var result = CollectReferencedCollections(map, additionalSources);
                mapReferencedCollections.Add(result);
            }

            if (Definition.Reduce != null)
            {
                EngineHandle.ExecuteWithReset(Definition.Reduce, "reduce");
                if (_jsEngineType == JavaScriptEngineType.V8)
                    _engineForParsing.ExecuteWithReset(Definition.Reduce);
            }

            return mapReferencedCollections;
        }

        private MapMetadata CollectReferencedCollections(string code, string additionalSources)
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
                catch {}
            }

            try
            {
                loadVisitor.Visit(program);
            }
            catch {}

            return new MapMetadata
            {
                ReferencedCollections = loadVisitor.ReferencedCollection,
                HasCompareExchangeReferences = loadVisitor.HasCompareExchangeReferences
            };
        }

        private JsHandle GetDefinitions()
        {
            var definitions = EngineHandle.GetGlobalProperty(GlobalDefinitions);

            if (definitions.IsNull || definitions.IsUndefined || definitions.IsObject == false)
                ThrowIndexCreationException($"is missing its '{GlobalDefinitions}' global variable, are you modifying it in your script?");

            if (definitions.GetProperty(MapsProperty).IsUndefined)
                ThrowIndexCreationException($"is missing its '{MapsProperty}' property, are you modifying it in your script?");

            return definitions;
        }

        private ObjectInstance GetDefinitionsForParsing()
        {
            var definitionsObj = _engineForParsing.GetGlobalProperty(GlobalDefinitions).Jint.Item;

            if (definitionsObj.IsNull() || definitionsObj.IsUndefined() || definitionsObj.IsObject() == false)
                ThrowIndexCreationException($"is missing its '{GlobalDefinitions}' global variable, are you modifying it in your script?");

            var definitions = definitionsObj.AsObject();
            if (definitions.HasProperty(MapsProperty) == false)
                ThrowIndexCreationException("is missing its 'globalDefinition.maps' property, are you modifying it in your script?");

            return definitions;
        }
    }
}
