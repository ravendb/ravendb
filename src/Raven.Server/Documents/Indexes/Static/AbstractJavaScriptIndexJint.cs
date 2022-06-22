using System;
using System.Collections.Generic;
using System.Text;
using Esprima;
using Jint;
using Jint.Native.Function;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.Static.JavaScript.Jint;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Extensions.Jint;

namespace Raven.Server.Documents.Indexes.Static;

public abstract class AbstractJavaScriptIndexJint : AbstractJavaScriptIndex<JsHandleJint>
{
    public JintEngineEx EngineEx;
    public Engine Engine;

    protected AbstractJavaScriptIndexJint(IndexDefinition definition, RavenConfiguration configuration, Action<List<string>> modifyMappingFunctions, string mapCode,
        long indexVersion)
        : base(definition)
    {
        // we create the engine instance directly instead of using SingleRun
        // because the index is single threaded and long lived
        var refResolver = new JintPreventResolvingTasksReferenceResolver();
        EngineEx = new JintEngineEx(configuration, refResolver);
        Engine = EngineEx.Engine;
        _engineForParsing = EngineEx;
        EngineHandle = EngineEx;
        JsUtils = JavaScriptUtilsJint.Create(null, EngineEx);
        JsIndexUtils = new JavaScriptIndexUtilsJint(JsUtils, Engine);
        Initialize(modifyMappingFunctions, mapCode, indexVersion);
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

        var mapReferencedCollections = InitializeEngineInternal(this.EngineHandle, this.Definition, maps, mapCode);
        return mapReferencedCollections;
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
