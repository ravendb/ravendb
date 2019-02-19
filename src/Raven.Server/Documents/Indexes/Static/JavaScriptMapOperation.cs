using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Esprima.Ast;
using Jint;
using Jint.Native;
using Jint.Native.Array;
using Jint.Native.Function;
using Jint.Runtime;
using Jint.Runtime.Environments;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Patch;
using Raven.Server.Extensions;

namespace Raven.Server.Documents.Indexes.Static
{

    public class JavaScriptMapOperation
    {
        public FunctionInstance MapFunc;

        public bool HasDynamicReturns;

        public HashSet<string> Fields = new HashSet<string>();
        public Dictionary<string, IndexFieldOptions> FieldOptions = new Dictionary<string, IndexFieldOptions>();
        private readonly Engine _engine;
        private readonly JintPreventResolvingTasksReferenceResolver _resolver;
        private readonly JsValue[] _oneItemArray = new JsValue[1];

        public string IndexName { get; set; }


        public JavaScriptMapOperation(Engine engine, JintPreventResolvingTasksReferenceResolver resolver)
        {
            _engine = engine;
            _resolver = resolver;
        }

        public IEnumerable IndexingFunction(IEnumerable<object> items)
        {
            try
            {
                foreach (var item in items)
                {
                    _engine.ResetCallStack();
                    _engine.ResetStatementsCount();
                    _engine.ResetTimeoutTicks();

                    if (JavaScriptIndexUtils.GetValue(_engine, item, out JsValue jsItem) == false)
                        continue;
                    {
                        _oneItemArray[0] = jsItem;
                        try
                        {
                            jsItem = MapFunc.Call(JsValue.Null, _oneItemArray);
                        }
                        catch (JavaScriptException jse)
                        {
                            var (message, success) = JavaScriptIndexFuncException.PrepareErrorMessageForJavaScriptIndexFuncException(MapString, jse);
                            if (success == false)
                                throw new JavaScriptIndexFuncException($"Failed to execute {MapString}", jse);
                            throw new JavaScriptIndexFuncException($"Failed to execute map script, {message}", jse);
                        }
                        catch (Exception e)
                        {
                            throw new JavaScriptIndexFuncException($"Failed to execute {MapString}", e);
                        }
                        if (jsItem.IsArray())
                        {
                            var array = jsItem.AsArray();
                            foreach (var (prop, val) in array.GetOwnProperties())
                            {
                                if (prop == "length")
                                    continue;
                                yield return val.Value;
                            }
                        }
                        else if (jsItem.IsObject())
                        {
                            yield return jsItem.AsObject();
                        }
                        // we ignore everything else by design, we support only
                        // objects and arrays, anything else is discarded
                    }

                    _resolver.ExplodeArgsOn(null, null);

                }
            }
            finally
            {
                _oneItemArray[0] = null;
            }
        }

        public void Analyze(Engine engine)
        {
            HasDynamicReturns = false;

            if (!(MapFunc is ScriptFunctionInstance sfi))
                return;

            var theFuncAst = sfi.FunctionDeclaration;

            var res = CheckIfSimpleMapExpression(engine, theFuncAst);
            if (res != null)
            {
                MapFunc = res.Value.Function;
                theFuncAst = res.Value.FunctionAst;
            }

            foreach (var returnStatement in JavaScriptIndexUtils.GetReturnStatements(theFuncAst.Body))
            {
                if (returnStatement.Argument == null) // return;
                    continue;

                if (!(returnStatement.Argument is ObjectExpression oe))
                {
                    HasDynamicReturns = true;
                    continue;
                }
                //If we got here we must validate that all return statements have the same structure.
                //Having zero fields means its the first return statements we encounter that has a structure.
                if (Fields.Count == 0)
                {
                    foreach (var prop in oe.Properties)
                    {
                        var fieldName = prop.Key.GetKey();
                        if (fieldName == "_")
                            HasDynamicReturns = true;

                        Fields.Add(fieldName);
                    }
                }
                else if (CompareFields(oe) == false)
                {
                    throw new InvalidOperationException($"Index {IndexName} contains different return structure from different code paths," +
                                                        $" expected properties: {string.Join(", ", Fields)} but also got:{string.Join(", ", oe.Properties.Select(x => x.Key.GetKey()))}");
                }
            }
        }

        private (FunctionInstance Function, IFunction FunctionAst)? CheckIfSimpleMapExpression(Engine engine, IFunction function)
        {
            var field = function.TryGetFieldFromSimpleLambdaExpression();
            if (field == null)
                return null;
            var properties = new List<Property>
            {
                new Property(PropertyKind.Data, new Identifier(field), false,
                    new StaticMemberExpression(new Identifier("self"), new Identifier(field)), false, false)
            };

            if (MoreArguments != null)
            {
                for (int i = 0; i < MoreArguments.GetLength(); i++)
                {
                    var arg = MoreArguments.Get(i.ToString()).As<FunctionInstance>();

                    if (!(arg is ScriptFunctionInstance sfi))
                        continue;
                    var moreFuncAst = sfi.FunctionDeclaration;
                    field = moreFuncAst.TryGetFieldFromSimpleLambdaExpression();
                    if (field != null)
                    {
                        properties.Add(new Property(PropertyKind.Data, new Identifier(field), false,
                        new StaticMemberExpression(new Identifier("self"), new Identifier(field)), false, false));

                    }
                }
            }

            var functionExp = new FunctionExpression(function.Id, new List<INode> { new Identifier("self") },
                new BlockStatement(new List<StatementListItem>
                {
                    new ReturnStatement(new ObjectExpression(properties))
                }), false, function.HoistingScope, function.Strict);
            var functionObject = new ScriptFunctionInstance(
                    engine,
                    functionExp,
                    LexicalEnvironment.NewDeclarativeEnvironment(engine, engine.ExecutionContext.LexicalEnvironment),
                    function.Strict
                )
            { Extensible = true };
            return (functionObject, functionExp);
        }

        public HashSet<CollectionName> ReferencedCollections { get; set; } = new HashSet<CollectionName>();
        public ArrayInstance MoreArguments { get; set; }
        public RavenConfiguration Configuration { get; set; }
        public string MapString { get; internal set; }

        private bool CompareFields(ObjectExpression oe)
        {
            if (Fields.Count != oe.Properties.Count())
                return false;
            foreach (var p in oe.Properties)
            {
                if (Fields.Contains(p.Key.GetKey()) == false)
                    return false;
            }

            return true;
        }
    }

}
