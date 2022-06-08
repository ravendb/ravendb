using System;
using System.Collections.Generic;
using System.Text;
using Esprima;
using Jint;
using Jint.Native.Function;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.Static.JavaScript.Jint;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Extensions.Jint;

namespace Raven.Server.Documents.Indexes.Static;

public abstract class AbstractJavaScriptIndexJint : AbstractJavaScriptIndex<JsHandleJint>
{
    public JintEngineEx EngineEx;
    public Engine Engine;

    protected AbstractJavaScriptIndexJint(IndexDefinition definition, RavenConfiguration configuration, Action<List<string>> modifyMappingFunctions, string mapCode, long indexVersion)
        : base(definition)
    {
        // we create the engine instance directly instead of using SingleRun
        // because the index is single threaded and long lived
        var refResolver = new JintPreventResolvingTasksReferenceResolver();
        EngineEx = new JintEngineEx(configuration, refResolver);
        Engine = EngineEx.Engine;
        _engineForParsing = EngineEx.Engine;
        EngineHandle = EngineEx;
        JsUtils = JavaScriptUtilsJint.Create(null, EngineEx);
        JsIndexUtils = new JavaScriptIndexUtilsJint(JsUtils, Engine);

        lock (EngineHandle)
        {
            Initialize(modifyMappingFunctions, mapCode, indexVersion);
        }
    }

    public override IDisposable DisableConstraintsOnInit()
    {
        return EngineHandle.DisableConstraints();
    }
    
    public override JavaScriptReduceOperation<JsHandleJint> CreateJavaScriptReduceOperation(ScriptFunctionInstance groupByKeyForParsingJint, JsHandleJint reduce, JsHandleJint groupByKey, long indexVersion)
    {
        return new JavaScriptReduceOperationJint(this, JsIndexUtils, groupByKeyForParsingJint, Engine, reduce, groupByKey, indexVersion)
        { ReduceString = Definition.Reduce };
    }

    protected override List<MapMetadata> InitializeEngine(List<string> maps, string mapCode)
    {
        OnInitializeEngine();

        EngineHandle.ExecuteWithReset(Code, "Code");
        EngineHandle.ExecuteWithReset(mapCode, "MapCode");
        ////TODO: egor add those to v8
      //  _engineForParsing.ExecuteWithReset(Code);
       // _engineForParsing.ExecuteWithReset(mapCode);

        var sb = new StringBuilder();
        if (Definition.AdditionalSources != null)
        {
            foreach (var script in Definition.AdditionalSources.Values)
            {
                EngineHandle.ExecuteWithReset(script);
                // TODO: egor add _engineFOrParsing
                sb.Append(Environment.NewLine);
                sb.AppendLine(script);
            }
        }

        var additionalSources = sb.ToString();
        var mapReferencedCollections = new List<MapMetadata>();
        foreach (var map in maps)
        {
            var result = ExecuteCodeAndCollectReferencedCollections(map, additionalSources);
            mapReferencedCollections.Add(result);
        }

        if (Definition.Reduce != null)
        {
            EngineHandle.ExecuteWithReset(Definition.Reduce, "reduce");
             //   _engineForParsing.ExecuteWithReset(Definition.Reduce);
        }

        return mapReferencedCollections;
    }
    private MapMetadata ExecuteCodeAndCollectReferencedCollections(string code, string additionalSources)
    {

        var javascriptParser = new JavaScriptParser(code, DefaultParserOptions);
        var program = javascriptParser.ParseScript();
        //   engine.ExecuteWithReset(program);
        EngineHandle.ExecuteWithReset(code, "map");
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

    public override JsHandleJint ConvertToJsHandle(object value)
    {
        switch (value)
        {
            case null:
                return EngineEx.ImplicitNull;

            case DynamicNullObject dno:
                return dno.IsExplicitNull ? EngineEx.ExplicitNull : EngineEx.ImplicitNull;

            case DynamicBlittableJson dbj:
                var jintBoi = new BlittableObjectInstanceJint(EngineEx, null, dbj.BlittableJson, id: null, lastModified: null, changeVector: null);
                return EngineEx.FromObjectGen(jintBoi);

            default:
                return JsUtils.TranslateToJs(context: null, value);
        }
    }

    public override JsHandleJint GetRecursiveJsFunctionInternal(JsHandleJint[] args)
    {
        var item = args[0].Item;
        var func = args[1].Item as ScriptFunctionInstance;

        if (func == null)
            throw new ArgumentException("The second argument in recurse(item, func) must be an arrow function.");

        var result = new RecursiveJsFunctionJint(Engine, item, func).Execute();
        return new JsHandleJint(result);
    }
}
