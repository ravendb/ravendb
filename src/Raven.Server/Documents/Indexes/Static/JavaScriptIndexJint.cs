using System.Collections.Generic;
using Jint;
using Jint.Native.Function;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Patch.Jint;

namespace Raven.Server.Documents.Indexes.Static;

public class JavaScriptIndexJint : AbstractJavaScriptIndexJint, IJavaScriptIndex<JsHandleJint>
{
    public JavaScriptIndexJint(IndexDefinition definition, RavenConfiguration configuration, long indexVersion) : base(definition, configuration, modifyMappingFunctions: null, JavaScriptIndexHelper.GetMapCode(), indexVersion)
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

    public JsHandleJint GetDocumentId(JsHandleJint self, JsHandleJint[] args)
    {
        JavaScriptIndexHelper.RegisterJavaScriptUtils(JsUtils);

        return JsUtils.GetDocumentId(self, args);
    }

    public JsHandleJint AttachmentsFor(JsHandleJint self, JsHandleJint[] args)
    {
        JavaScriptIndexHelper.RegisterJavaScriptUtils(JsUtils);
        return JsUtils.AttachmentsFor(self, args);
    }

    public JsHandleJint MetadataFor(JsHandleJint self, JsHandleJint[] args)
    {
        JavaScriptIndexHelper.RegisterJavaScriptUtils(JsUtils);
        return JsUtils.GetMetadata(self, args);
    }

    public JsHandleJint TimeSeriesNamesFor(JsHandleJint self, JsHandleJint[] args)
    {
        JavaScriptIndexHelper.RegisterJavaScriptUtils(JsUtils);

        return JsUtils.GetTimeSeriesNamesFor(self, args);
    }

    public JsHandleJint CounterNamesFor(JsHandleJint self, JsHandleJint[] args)
    {
        JavaScriptIndexHelper.RegisterJavaScriptUtils(JsUtils);

        return JsUtils.GetCounterNamesFor(self, args);
    }

    public JsHandleJint LoadAttachment(JsHandleJint self, JsHandleJint[] args)
    {
        JavaScriptIndexHelper.RegisterJavaScriptUtils(JsUtils);

        return JsUtils.LoadAttachment(self, args);
    }

    public JsHandleJint LoadAttachments(JsHandleJint self, JsHandleJint[] args)
    {
        JavaScriptIndexHelper.RegisterJavaScriptUtils(JsUtils);

        return JsUtils.LoadAttachments(self, args);
    }

    protected override void ProcessMaps(List<string> mapList, List<MapMetadata> mapReferencedCollections, out Dictionary<string, Dictionary<string, List<JavaScriptMapOperation<JsHandleJint>>>> collectionFunctions)
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

            collectionFunctions = new Dictionary<string, Dictionary<string, List<JavaScriptMapOperation<JsHandleJint>>>>();
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
                            collectionFunctions[mapCollection] = subCollectionFunctions = new Dictionary<string, List<JavaScriptMapOperation<JsHandleJint>>>();

                        if (subCollectionFunctions.TryGetValue(mapCollection, out var list) == false)
                            subCollectionFunctions[mapCollection] = list = new List<JavaScriptMapOperation<JsHandleJint>>();

                        if (map.HasProperty(MethodProperty) == false)
                            ThrowIndexCreationException($"map function #{i} is missing its {MethodProperty} property");

                        using (var func = map.GetProperty(MethodProperty))
                        {
                            if (func.IsFunction == false)
                                ThrowIndexCreationException($"map function #{i} {MethodProperty} property isn't a function");

                            var operation = new JavaScriptMapOperationJint(this, JsIndexUtils, funcForParsingJint, func, Definition.Name, mapList[i]);
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

                            //  operation.Analyze(_engineForParsing);
                            operation.Analyze(this.Engine);

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
