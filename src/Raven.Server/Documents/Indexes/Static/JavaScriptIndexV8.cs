using System.Collections.Generic;
using Jint;
using Jint.Native.Function;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Patch.V8;

namespace Raven.Server.Documents.Indexes.Static;

public class JavaScriptIndexV8 : AbstractJavaScriptIndexV8
{
    public JavaScriptIndexV8(IndexDefinition definition, RavenConfiguration configuration, long indexVersion) : base(definition, configuration, modifyMappingFunctions: null, JavaScriptIndexHelper.GetMapCode(), indexVersion)
    {
    }

    protected override void OnInitializeEngine()
    {
        base.OnInitializeEngine();

        EngineHandle.SetGlobalClrCallBack("getMetadata", MetadataFor); // for backward-compatibility only
        EngineHandle.SetGlobalClrCallBack("metadataFor", MetadataFor);
        EngineHandle.SetGlobalClrCallBack("attachmentsFor", AttachmentsFor);
        EngineHandle.SetGlobalClrCallBack("timeSeriesNamesFor", TimeSeriesNamesFor);
        EngineHandle.SetGlobalClrCallBack("counterNamesFor", CounterNamesFor);
        EngineHandle.SetGlobalClrCallBack("loadAttachment", LoadAttachment);
        EngineHandle.SetGlobalClrCallBack("loadAttachments", LoadAttachments);
        EngineHandle.SetGlobalClrCallBack("id", GetDocumentId);
    }

    public  JsHandleV8 GetDocumentId(JsHandleV8 self, JsHandleV8[] args)
    {
        JavaScriptIndexHelper.RegisterJavaScriptUtils(JsUtils);

        return JsUtils.GetDocumentId(self, args);
    }

    public  JsHandleV8 AttachmentsFor(JsHandleV8 self, JsHandleV8[] args)
    {
        JavaScriptIndexHelper.RegisterJavaScriptUtils(JsUtils);
        return JsUtils.AttachmentsFor(self, args);
    }

    public  JsHandleV8 MetadataFor(JsHandleV8 self, JsHandleV8[] args)
    {
        JavaScriptIndexHelper.RegisterJavaScriptUtils(JsUtils);
        return JsUtils.GetMetadata(self, args);
    }

    public  JsHandleV8 TimeSeriesNamesFor(JsHandleV8 self, JsHandleV8[] args)
    {
        JavaScriptIndexHelper.RegisterJavaScriptUtils(JsUtils);

        return JsUtils.GetTimeSeriesNamesFor(self, args);
    }

    public  JsHandleV8 CounterNamesFor(JsHandleV8 self, JsHandleV8[] args)
    {
        JavaScriptIndexHelper.RegisterJavaScriptUtils(JsUtils);

        return JsUtils.GetCounterNamesFor(self, args);
    }

    public  JsHandleV8 LoadAttachment(JsHandleV8 self, JsHandleV8[] args)
    {
        JavaScriptIndexHelper.RegisterJavaScriptUtils(JsUtils);

        return JsUtils.LoadAttachment(self, args);
    }

    public JsHandleV8 LoadAttachments(JsHandleV8 self, JsHandleV8[] args)
    {
        JavaScriptIndexHelper.RegisterJavaScriptUtils(JsUtils);

        return JsUtils.LoadAttachments(self, args);
    }

    //TODO: egor seems like we use JINT engine in V8, so we need to create dedicated method for jint or reuse the code in both engine types
    protected override void ProcessMaps(List<string> mapList, List<MapMetadata> mapReferencedCollections, out Dictionary<string, Dictionary<string, List<JavaScriptMapOperation<JsHandleV8>>>> collectionFunctions)
    {
        using (var maps = _definitions.GetProperty(MapsProperty))
        {
            if (maps.IsNull || maps.IsUndefined || maps.IsArray == false)
                ThrowIndexCreationException($"doesn't contain any map function or '{GlobalDefinitions}.{Maps}' was modified in the script");

            var mapCount = maps.ArrayLength;
            if (mapCount == 0)
                ThrowIndexCreationException($"doesn't contain any map functions or '{GlobalDefinitions}.{Maps}' was modified in the script");

            var mapsArrayForParsingJint = _definitionsForParsing.GetProperty(MapsProperty).Value;
            if (mapsArrayForParsingJint.IsNull() || mapsArrayForParsingJint.IsUndefined() || mapsArrayForParsingJint.IsArray() == false)
                ThrowIndexCreationException($"Jint doesn't contain any map function");

            var mapsJint = mapsArrayForParsingJint.AsArray();

            collectionFunctions = new Dictionary<string, Dictionary<string, List<JavaScriptMapOperation<JsHandleV8>>>>();
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
                            collectionFunctions[mapCollection] = subCollectionFunctions = new Dictionary<string, List<JavaScriptMapOperation<JsHandleV8>>>();

                        if (subCollectionFunctions.TryGetValue(mapCollection, out var list) == false)
                            subCollectionFunctions[mapCollection] = list = new List<JavaScriptMapOperation<JsHandleV8>>();

                        if (map.HasProperty(MethodProperty) == false)
                            ThrowIndexCreationException($"map function #{i} is missing its {MethodProperty} property");

                        using (var func = map.GetProperty(MethodProperty))
                        {
                            if (func.IsFunction == false)
                                ThrowIndexCreationException($"map function #{i} {MethodProperty} property isn't a function");

                            var operation = new JavaScriptMapOperationV8(this, JsIndexUtils, funcForParsingJint, func, Definition.Name, mapList[i]);
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
                            //TODO: egor why its gets jint engine even for v8?? should I make analyze for v8 as well? answer: seems like jint is used even in v8
                            operation.Analyze(_engineForParsing);

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
