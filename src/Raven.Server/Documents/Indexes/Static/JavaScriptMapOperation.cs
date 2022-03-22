using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using Esprima.Ast;
using Jint;
using Jint.Native.Array;
using Jint.Native.Function;
using Jint.Runtime.Environments;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide.JavaScript;
using Raven.Server.Extensions.Jint;
using Raven.Server.Documents.Indexes.Static.Utils;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.Jint;
using JintPreventResolvingTasksReferenceResolver = Raven.Server.Documents.Patch.Jint.JintPreventResolvingTasksReferenceResolver;
using V8Exception = V8.Net.V8Exception;
using JavaScriptException = Jint.Runtime.JavaScriptException;
using Raven.Server.Documents.Patch.V8;
using V8.Net;

namespace Raven.Server.Documents.Indexes.Static
{
    public partial class JavaScriptMapOperation
    {
        private readonly AbstractJavaScriptIndex _index;
        private readonly JavaScriptIndexUtils _jsIndexUtils;
        private readonly IJsEngineHandle _engineHandle;
        private JavaScriptEngineType _jsEngineType => _engineHandle.EngineType;
        private IJavaScriptEngineForParsing EngineForParsing { get; }
        protected readonly Engine _engineStaticJint;

        public FunctionInstance MapFuncJint;
        public JsHandle MapFunc;

        private readonly JintPreventResolvingTasksReferenceResolver _resolver;

        public bool HasDynamicReturns;

        public bool HasBoostedFields;

        public HashSet<string> Fields = new HashSet<string>();
        public Dictionary<string, IndexFieldOptions> FieldOptions = new Dictionary<string, IndexFieldOptions>();
        public string IndexName { get; set; }

        public JavaScriptMapOperation(AbstractJavaScriptIndex index, JavaScriptIndexUtils jsIndexUtils, FunctionInstance mapFuncJint, JsHandle mapFunc, string indexName, string mapString)
        {
            _index = index;
            EngineForParsing = jsIndexUtils.EngineForParsing;
            _engineStaticJint = (Engine)EngineForParsing;

            _jsIndexUtils = jsIndexUtils;
            _engineHandle = _jsIndexUtils.EngineHandle;

            MapFunc = new JsHandle(ref mapFunc);
            MapFuncJint = mapFuncJint ?? throw new ArgumentNullException(nameof(mapFuncJint));
            IndexName = indexName;
            MapString = mapString;

            if (_engineHandle.EngineType == JavaScriptEngineType.Jint)
                _resolver = ((JintEngineEx)_engineHandle).RefResolver;
        }

        ~JavaScriptMapOperation()
        {
            MapFunc.Dispose();
        }
        
        public IEnumerable<JsHandle> IndexingFunction(IEnumerable<object> items)
        {
            lock (_engineHandle)
            {
                _index._lastException = null;
                switch (_jsEngineType)
                {
                    case JavaScriptEngineType.Jint:
                        SetContextJint();
                        break;
                    case JavaScriptEngineType.V8:
                        SetContextV8();
                        break;
                    default:
                        throw new NotSupportedException($"Not supported JS engine kind '{_jsEngineType}'.");
                }

                var memorySnapshotName = "map";
                bool isMemorySnapshotMade = false;
                if (_engineHandle.IsMemoryChecksOn)
                {
                    _engineHandle.MakeSnapshot(memorySnapshotName);
                    isMemorySnapshotMade = true;
                }

                foreach (var item in items)
                {
                    _engineHandle.ResetCallStack();
                    _engineHandle.ResetConstraints();

                    if (_jsIndexUtils.GetValue(item, out JsHandle jsItem) == false)
                        continue;

                    if (jsItem.IsObject)
                    {
                        using (jsItem)
                        {
                            JsHandle jsRes = JsHandle.Empty;
                            try
                            {
                                if (!MapFunc.IsFunction)
                                {
                                    throw new JavaScriptIndexFuncException($"MapFunc is not a function");
                                }

                                jsRes = MapFunc.StaticCall(jsItem);
                                if (_index._lastException != null)
                                {
                                    ExceptionDispatchInfo.Capture(_index._lastException).Throw();
                                }
                                else
                                {
                                    jsRes.ThrowOnError();
                                }
                            }
                            catch (JavaScriptException jse)
                            {
                                ProcessRunException(jsRes, memorySnapshotName, isMemorySnapshotMade);
                                var (message, success) = JavaScriptIndexFuncException.PrepareErrorMessageForJavaScriptIndexFuncException(MapString, jse);
                                if (success == false)
                                    throw new JavaScriptIndexFuncException($"Failed to execute {MapString}", jse);
                                throw new JavaScriptIndexFuncException($"Failed to execute map script, {message}", jse);
                            }
                            catch (V8Exception jse)
                            {
                                ProcessRunException(jsRes, memorySnapshotName, isMemorySnapshotMade);
                                var (message, success) = JavaScriptIndexFuncException.PrepareErrorMessageForJavaScriptIndexFuncException(MapString, jse);
                                if (success == false)
                                    throw new JavaScriptIndexFuncException($"Failed to execute {MapString}", jse);
                                throw new JavaScriptIndexFuncException($"Failed to execute map script, {message}", jse);
                            }
                            catch (Exception e)
                            {
                                ProcessRunException(jsRes, memorySnapshotName, isMemorySnapshotMade);
                                throw new JavaScriptIndexFuncException($"Failed to execute {MapString}", e);
                            }
                            finally
                            {
                                _index._lastException = null;
                            }

                            using (jsRes)
                            {
                                if (jsRes.IsArray)
                                {
                                    var length = (uint)jsRes.ArrayLength;
                                    for (int i = 0; i < length; i++)
                                    {
                                        var arrItem = jsRes.GetProperty(i);
                                        using (arrItem)
                                        {
                                            if (arrItem.IsObject)
                                            {
                                                yield return arrItem; // being yield it is converted to blittable object and not disposed - so disposing it here
                                            }
                                            else
                                            {
                                                ProcessRunException(jsRes, memorySnapshotName, isMemorySnapshotMade);
                                                
                                                // this check should be to catch map errors
                                                throw new JavaScriptIndexFuncException($"Failed to execute {MapString}",
                                                    new Exception($"At least one of map results is not object: {jsRes.ToString()}"));
                                            }
                                        }
                                    }
                                }
                                else if (jsRes.IsObject)
                                {
                                    yield return jsRes; // being yield it is converted to blittable object and not disposed - so disposing it here
                                }
                                // we ignore everything else by design, we support only
                                // objects and arrays, anything else is discarded
                            }
                        }

                        _engineHandle.ForceGarbageCollection();
                        if (isMemorySnapshotMade)
                        {
                            _engineHandle.CheckForMemoryLeaks(memorySnapshotName, shouldRemove: false);
                        }
                    }
                    else
                    {
                        using (jsItem)
                            throw new JavaScriptIndexFuncException($"Failed to execute {MapString}", new Exception($"Entry item is not document: {jsItem.ToString()}"));
                    }

                    switch (_engineHandle.EngineType) // TODO [shlomo] why there is no SetArgs in indexes?
                    {
                        case JavaScriptEngineType.Jint:
                            _resolver.ExplodeArgsOn(null, null);
                            break;
                        case JavaScriptEngineType.V8:
                            break;
                        default:
                            throw new NotSupportedException($"Not supported JS engine kind '{_engineHandle.EngineType}'.");
                    }
                }
                
                if (isMemorySnapshotMade)
                {
                    _engineHandle.RemoveMemorySnapshot(memorySnapshotName);
                }
            }
        }

        private void ProcessRunException(JsHandle jsRes, string memorySnapshotName, bool isMemorySnapshotMade)
        {
            jsRes.Dispose();

            _engineHandle.ForceGarbageCollection();
            if (isMemorySnapshotMade)
            {
                _engineHandle.CheckForMemoryLeaks(memorySnapshotName);
            }
        }
        
        public void Analyze(Engine engine)
        {
            HasDynamicReturns = false;
            HasBoostedFields = false;

            IFunction theFuncAst;
            switch (MapFuncJint)
            {
                case ScriptFunctionInstance sfi:
                    theFuncAst = sfi.FunctionDeclaration;
                    break;

                default:
                    return;
            }

            var res = CheckIfSimpleMapExpression(engine, theFuncAst);
            if (res != null)
            {
                MapFunc.Set(res.Value.Function);
                theFuncAst = res.Value.FunctionAst;
            }

            foreach (var returnStatement in JavaScriptIndexUtils.GetReturnStatements(theFuncAst))
            {
                if (returnStatement.Argument == null) // return;
                    continue;

                switch (returnStatement.Argument)
                {
                    case ObjectExpression oe:

                        //If we got here we must validate that all return statements have the same structure.
                        //Having zero fields means its the first return statements we encounter that has a structure.
                        if (Fields.Count == 0)
                        {
                            foreach (var prop in oe.Properties)
                            {
                                if (prop is Property property)
                                {
                                    var fieldName = property.GetKey(engine);
                                    var fieldNameAsString = fieldName.AsString();
                                    if (fieldName == "_")
                                        HasDynamicReturns = true;

                                    Fields.Add(fieldNameAsString);

                                    var fieldValue = property.Value;
                                    if (IsBoostExpression(fieldValue))
                                        HasBoostedFields = true;
                                }
                            }
                        }
                        else if (CompareFields(oe) == false)
                        {
                            throw new InvalidOperationException($"Index {IndexName} contains different return structure from different code paths," +
                                                                $" expected properties: {string.Join(", ", Fields)} but also got:{string.Join(", ", oe.Properties.Select(x => x.GetKey(engine)))}");
                        }

                        break;

                    case CallExpression ce:

                        if (IsBoostExpression(ce))
                            HasBoostedFields = true;
                        else
                            HasDynamicReturns = true;

                        break;

                    default:
                        HasDynamicReturns = true;
                        break;
                }
            }

            static bool IsBoostExpression(Expression expression)
            {
                return expression is CallExpression ce && ce.Callee is Identifier identifier && identifier.Name == "boost";
            }
        }

        protected (JsHandle Function, IFunction FunctionAst)? CheckIfSimpleMapExpression(Engine engine, IFunction function)
        {
            var field = function.TryGetFieldFromSimpleLambdaExpression();
            if (field == null)
                return null;
            var properties = new List<Expression>
            {
                new Property(PropertyKind.Data, new Identifier(field), false,
                    new StaticMemberExpression(new Identifier("self"), new Identifier(field), false), false, false)
            };
            var fields = new List<string> {field};

            if (MoreArguments != null)
            {
                for (int i = 0; i < MoreArguments.Length; i++)
                {
                    var arg = MoreArguments.Get(i.ToString()).As<FunctionInstance>();

                    if (!(arg is ScriptFunctionInstance sfi))
                        continue;
                    var moreFuncAst = sfi.FunctionDeclaration;
                    field = moreFuncAst.TryGetFieldFromSimpleLambdaExpression();
                    if (field != null)
                    {
                        properties.Add(new Property(PropertyKind.Data, new Identifier(field), false,
                        new StaticMemberExpression(new Identifier("self"), new Identifier(field), false), false, false));
                        fields.Add(field);
                    }
                }
            }

            var functionExp = new FunctionExpression(
                function.Id,
                NodeList.Create(new List<Expression> { new Identifier("self") }),
                new BlockStatement(NodeList.Create(new List<Statement>
                {
                    new ReturnStatement(new ObjectExpression(NodeList.Create(properties)))
                })),
                generator: false,
                function.Strict,
                async: false);

            var jsEngineType = MapFunc.EngineType;
            JsHandle functionObject; 
            switch (jsEngineType)
            {
                case JavaScriptEngineType.Jint:
                    functionObject = new JsHandle(new ScriptFunctionInstance(
                        engine,
                        functionExp,
                        JintEnvironment.NewDeclarativeEnvironment(engine, engine.ExecutionContext.LexicalEnvironment), //TODO [shlomo] restore whne Jint gets updated
                        function.Strict
                    ));
                    break;
                case JavaScriptEngineType.V8:
                    var objectBody = "";
                    foreach (var fn in fields)
                    {
                        if (objectBody != "")
                            objectBody += ", ";
                        objectBody += fn + ": d." + fn;
                    }
                    var newMapCode = "d => { return {" + objectBody + "}; }";
                    functionObject = new JsHandle(((V8EngineEx)_engineHandle).ExecuteExprWithReset(newMapCode, "newMapCode"));
                    break;
                default:
                    throw new NotSupportedException($"Not supported JS engine kind '{jsEngineType}'.");
            }
            return (functionObject, functionExp);
        }

        public ArrayInstance MoreArguments { get; set; }
        public string MapString { get; internal set; }

        protected bool CompareFields(ObjectExpression oe)
        {
            if (Fields.Count != oe.Properties.Count)
                return false;
            foreach (var p in oe.Properties)
            {
                var key = p.GetKey(_engineStaticJint);
                var keyAsString = key.AsString();
                if (Fields.Contains(keyAsString) == false)
                    return false;
            }

            return true;
        }
    }
}
