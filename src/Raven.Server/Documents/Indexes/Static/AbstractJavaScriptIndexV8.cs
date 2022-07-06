using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Jint;
using Jint.Native.Function;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.Static.JavaScript.V8;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Documents.Patch.V8;
using Raven.Server.Extensions.Jint;
using V8.Net;
using Voron.Util;

namespace Raven.Server.Documents.Indexes.Static;

public abstract class AbstractJavaScriptIndexV8 : AbstractJavaScriptIndex<JsHandleV8>, IJavaScriptContext
{
    public V8EngineEx EngineEx;
    public V8Engine Engine;

    private JavaScriptUtilsV8 JsUtilsV8;

    protected AbstractJavaScriptIndexV8(IndexDefinition definition, RavenConfiguration configuration, Action<List<string>> modifyMappingFunctions, string mapCode,
        long indexVersion, CancellationToken token)
        : base(definition)
    {
        var engineEx = V8EngineEx.GetEngine(configuration, jsContext: this, token);
        //TODO: egor jsContext is passed to only set "last exception"
        Engine = engineEx.Engine;
        EngineHandle = EngineEx = engineEx;
        _engineForParsing = new JintEngineEx(configuration);
        JsUtils = JavaScriptUtilsV8.Create(null, EngineEx);
        JsIndexUtils = new JavaScriptIndexUtilsV8(JsUtils, _engineForParsing.Engine);

        Initialize(modifyMappingFunctions, mapCode, indexVersion);
    }

    protected abstract override void ProcessMaps(List<string> mapList, List<MapMetadata> mapReferencedCollections,
        out Dictionary<string, Dictionary<string, List<JavaScriptMapOperation<JsHandleV8>>>> collectionFunctions);


    public override IDisposable DisableConstraintsOnInit()
    {
        var d1 = EngineHandle.DisableConstraints();
        var d2 = _engineForParsing.Engine.DisableMaxStatements();
        return new DisposableAction(() => { d1.Dispose(); d2.Dispose(); });
    }

    //public override ObjectInstance GetDefinitionsForParsingJint()
    //{
    //    // TODO: egor need to use _definitions only?
    //    // noop

    //    return null;
    //}
    public override JavaScriptReduceOperation<JsHandleV8> CreateJavaScriptReduceOperation(ScriptFunctionInstance groupByKeyForParsingJint, JsHandleV8 reduce, JsHandleV8 groupByKey, long indexVersion)
    {
        return new JavaScriptReduceOperationV8(this, JsIndexUtils, groupByKeyForParsingJint, _engineForParsing.Engine, reduce, groupByKey, indexVersion)
            { ReduceString = Definition.Reduce };
    }
    protected override List<MapMetadata> InitializeEngine(List<string> maps, string mapCode)
    {
        OnInitializeEngine();

        var mapReferencedCollections = InitializeEngineInternal(EngineHandle, this.Definition, maps, mapCode);
        InitializeEngineInternal(_engineForParsing, this.Definition, maps, mapCode);
        return mapReferencedCollections;
    }

    public override JsHandleV8 ConvertToJsHandle(object value)
    {
        switch (value)
        {
            case null:
                return EngineEx.ImplicitNull;

            case DynamicNullObject dno:
                return dno.IsExplicitNull ? EngineEx.ExplicitNull : EngineEx.ImplicitNull;

            case DynamicBlittableJson dbj:
                var jintBoi = new BlittableObjectInstanceV8((JavaScriptUtilsV8)JsUtils, null, dbj.BlittableJson, id: null, lastModified: null, changeVector: null);

                return jintBoi.CreateObjectBinder(true);
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
