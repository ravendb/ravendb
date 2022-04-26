using System;
using System.Collections.Generic;
using System.Text;
using Jint;
using Jint.Native.Function;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.Static.JavaScript.V8;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.V8;
using Raven.Server.Extensions.Jint;
using V8.Net;
using Voron.Util;

namespace Raven.Server.Documents.Indexes.Static;

public abstract class AbstractJavaScriptIndexV8 : AbstractJavaScriptIndex<JsHandleV8>, IJavaScriptContext
{
    public V8EngineEx EngineEx;
    public V8Engine Engine;
    private PoolWithLevels<V8EngineEx>.PooledValue _scriptEngineV8Pooled;
    public V8EngineEx.ContextEx _contextExV8;
    private JavaScriptUtilsV8 JsUtilsV8;
    protected AbstractJavaScriptIndexV8(IndexDefinition definition, RavenConfiguration configuration, Action<List<string>> modifyMappingFunctions, string mapCode, long indexVersion)
        : base(definition, configuration, mapCode, indexVersion)
    {
        _scriptEngineV8Pooled = V8EngineEx.GetPool(configuration).GetValue();
        var engineEx = _scriptEngineV8Pooled.Value;
        //TODO: egor this is passed to only set "last exception"
        _contextExV8 = engineEx.CreateAndSetContextEx(configuration, jsContext: this);
        //TODO: egor this is set in line above??? not needed here?
        engineEx.Context = _contextExV8;
        Engine = EngineEx.Engine;
        EngineHandle = engineEx;
        EngineEx = engineEx;

        _engineForParsing = new Engine();
        JsUtils = JavaScriptUtilsV8.Create(null, EngineEx);
        JsIndexUtils = new JavaScriptIndexUtilsV8(JsUtils, _engineForParsing);

        lock (EngineHandle)
        {
            Initialize(modifyMappingFunctions);
        }
    }

    protected abstract override void ProcessMaps(List<string> mapList, List<MapMetadata> mapReferencedCollections,
        out Dictionary<string, Dictionary<string, List<JavaScriptMapOperation<JsHandleV8>>>> collectionFunctions);


    public override IDisposable DisableConstraintsOnInit()
    {
        var d1 = EngineHandle.DisableConstraints();
        var d2 = _engineForParsing.DisableMaxStatements();
        return new DisposableAction(() => { d1.Dispose(); d2.Dispose(); });
    }

    //public override ObjectInstance GetDefinitionsForParsingJint()
    //{
    //    // TODO: egor need to use _definitions only?
    //    // noop

    //    return null;
    //}
    public override JavaScriptReduceOperation<JsHandleV8> CreateJavaScriptReduceOperation(ScriptFunctionInstance groupByKeyForParsingJint, JsHandleV8 reduce, JsHandleV8 groupByKey)
    {
        return new JavaScriptReduceOperationV8(this, JsIndexUtils, groupByKeyForParsingJint, _engineForParsing, reduce, groupByKey, _indexVersion)
            { ReduceString = Definition.Reduce };
    }
    protected override List<MapMetadata> InitializeEngine(List<string> maps)
    {
        OnInitializeEngine();

        EngineHandle.ExecuteWithReset(Code, "Code");
        EngineHandle.ExecuteWithReset(MapCode, "MapCode");
        //TODO: egor add those to v8
        //_engineForParsing.ExecuteWithReset(Code);
        //_engineForParsing.ExecuteWithReset(MapCode);

        var sb = new StringBuilder();
        if (Definition.AdditionalSources != null)
        {
            foreach (var kvpScript in Definition.AdditionalSources)
            {
                var script = kvpScript.Value;
                EngineHandle.ExecuteWithReset(script, $"./{Definition.Name}/additionalSource/{kvpScript.Key}");
                //   _engineForParsing.ExecuteWithReset(script);
                sb.Append(Environment.NewLine);
                sb.AppendLine(script);
            }
        }

        var additionalSources = sb.ToString();
        var mapReferencedCollections = new List<MapMetadata>();
        foreach (var map in maps)
        {
            EngineHandle.ExecuteWithReset(map, "map");
            //     _engineForParsing.ExecuteWithReset(map);
            var result = CollectReferencedCollections(map, additionalSources);
            mapReferencedCollections.Add(result);
        }

        if (Definition.Reduce != null)
        {
            EngineHandle.ExecuteWithReset(Definition.Reduce, "reduce");
            //    _engineForParsing.ExecuteWithReset(Definition.Reduce);
        }

        return mapReferencedCollections;
    }

    public override JsHandleV8 ConvertToJsHandle(object value)
    {
        switch (value)
        {
            case null:
                return EngineEx.ImplicitNull();

            case DynamicNullObject dno:
                return dno.IsExplicitNull ? EngineEx.ExplicitNull() : EngineEx.ImplicitNull();

            case DynamicBlittableJson dbj:
                var jintBoi = new BlittableObjectInstanceV8((JavaScriptUtilsV8)JsUtils, null, dbj.BlittableJson, id: null, lastModified: null, changeVector: null);
                return EngineEx.FromObjectGen(jintBoi);

            default:
                return JsUtils.TranslateToJs(context: null, value);
        }
    }

    public override JsHandleV8 GetRecursiveJsFunctionInternal(JsHandleV8[] args)
    {

        var item = args[0];
        var func = args[1];

        if (!func.IsFunction)
            throw new ArgumentException("The second argument in recurse(item, func) must be an arrow function.");

        var result = new RecursiveJsFunctionV8(Engine, item.Item, func.Item).Execute();
        return new JsHandleV8(ref result);
    }

    public Exception LastException { get; set; }
}
