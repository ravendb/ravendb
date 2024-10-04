using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Acornima.Ast;
using Jint;
using Jint.Native;
using Jint.Native.Function;
using Jint.Runtime;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Patch;
using Raven.Server.Extensions;

namespace Raven.Server.Documents.Indexes.Static
{
    public sealed class JavaScriptMapOperation
    {
        public Function MapFunc;

        public bool HasDynamicReturns;

        public bool HasBoostedFields;

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
                    _engine.Advanced.ResetCallStack();
                    _engine.Constraints.Reset();

                    if (JavaScriptIndexUtils.GetValue(_engine, item, out JsValue jsItem) == false)
                        continue;
                    {
                        _oneItemArray[0] = jsItem;
                        try
                        {
                            jsItem = MapFunc.Call(JsValue.Null, _oneItemArray);
                        }
                        catch (StatementsCountOverflowException e)
                        {
                            throw new Client.Exceptions.Documents.Patching.JavaScriptException(
                                $"The maximum number of statements executed has been reached. You can configure it by modifying the configuration option: '{RavenConfiguration.GetKey(x => x.Indexing.MaxStepsForScript)}'.",
                                e);
                        }
                        catch (JavaScriptException jse) when (jse.Message.Contains("String compilation has been disabled in engine options"))
                        {
                            throw new Client.Exceptions.Documents.Patching.JavaScriptException(
                                $"String compilation has been disabled in engine options. You can configure it by modifying the configuration option: '{RavenConfiguration.GetKey(x => x.Indexing.AllowStringCompilation)}'.", jse);
                            ;
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
                            foreach (var val in array)
                            {
                                yield return val;
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
            HasBoostedFields = false;

            IFunction theFuncAst;
            switch (MapFunc)
            {
                case ScriptFunction sfi:
                    theFuncAst = sfi.FunctionDeclaration;
                    break;

                default:
                    return;
            }

            var res = CheckIfSimpleMapExpression(engine, theFuncAst);
            if (res != null)
            {
                MapFunc = res.Value.Function;
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
                                                                $" expected properties: {string.Join(", ", Fields)} but also got:{string.Join(", ", oe.Properties.OfType<IProperty>().Select(x => x.GetKey(engine)))}");
                        }

                        break;

                    case CallExpression ce:

                        if (IsBoostExpression(ce))
                        {
                            HasBoostedFields = true;

                            if (ce.Arguments[0] is ObjectExpression oe)
                                AddObjectFieldsToIndexFields(oe);
                        }
                        else if (IsArrowFunctionExpressionWithObjectExpressionBody(ce, out var oea))
                            AddObjectFieldsToIndexFields(oea);
                        else
                            HasDynamicReturns = true;
                        break;

                    default:
                        HasDynamicReturns = true;
                        break;
                }
            }

            static bool IsBoostExpression(Node expression)
            {
                return expression is CallExpression ce && ce.Callee is Identifier identifier && identifier.Name == "boost";
            }

            static bool IsArrowFunctionExpressionWithObjectExpressionBody(CallExpression callExpression, out ObjectExpression oea)
            {
                oea = null;
                if (callExpression.Arguments.Count == 1 && callExpression.Arguments.AsNodes()[0] is ArrowFunctionExpression afe && afe.Body is ObjectExpression _oea)
                    oea = _oea;

                return oea != null;
            }

            void AddObjectFieldsToIndexFields(ObjectExpression oe)
            {
                foreach (var prop in oe.Properties)
                {
                    if (prop is Property property)
                    {
                        var fieldName = property.GetKey(engine);
                        var fieldNameAsString = fieldName.AsString();

                        Fields.Add(fieldNameAsString);
                    }
                }
            }
        }

        private (Function Function, IFunction FunctionAst)? CheckIfSimpleMapExpression(Engine engine, IFunction function)
        {
            var field = function.TryGetFieldFromSimpleLambdaExpression();
            if (field == null)
                return null;

            Identifier self = new Identifier("self");

            var properties = new List<Node>
            {
                new ObjectProperty(PropertyKind.Init, new Identifier(field),
                    new MemberExpression(self, new Identifier(field), optional: false, computed: false), false, false, false)
            };

            if (MoreArguments != null)
            {
                for (uint i = 0; i < MoreArguments.Length; i++)
                {
                    if (MoreArguments[i] is not ScriptFunction sfi)
                        continue;

                    var moreFuncAst = sfi.FunctionDeclaration;
                    field = moreFuncAst.TryGetFieldFromSimpleLambdaExpression();
                    if (field != null)
                    {
                        properties.Add(new ObjectProperty(PropertyKind.Init, new Identifier(field), new MemberExpression(self, new Identifier(field), optional: false, computed: false), false, false, false));
                    }
                }
            }

            var strict = (function.Body as FunctionBody)?.Strict ?? false;

            var functionExp = new FunctionExpression(
                function.Id,
                NodeList.From(new List<Node> { self }),
                new FunctionBody(NodeList.From(new List<Statement>
                {
                    new ReturnStatement(new ObjectExpression(NodeList.From(properties)))
                }), strict),
                generator: false,
                async: false);

            var functionObject = new ScriptFunction(
                engine,
                functionExp,
                strict
            );

            return (functionObject, functionExp);
        }

        public JsArray MoreArguments { get; set; }
        public string MapString { get; internal set; }

        private bool CompareFields(ObjectExpression oe)
        {
            if (Fields.Count != oe.Properties.Count)
                return false;
            foreach (var p in oe.Properties)
            {
                if (p is IProperty property)
                {
                    var key = property.GetKey(_engine);
                    var keyAsString = key.AsString();
                    if (Fields.Contains(keyAsString) == false)
                        return false;
                }
            }

            return true;
        }
    }
}
