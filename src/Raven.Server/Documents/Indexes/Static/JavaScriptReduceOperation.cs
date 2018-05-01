using System;
using System.Collections;
using System.Collections.Generic;
using Jint;
using Jint.Native;
using Jint.Native.Function;
using Raven.Server.Documents.Patch;
using Raven.Server.Extensions;
using Esprima.Ast;
using Jint.Runtime;

namespace Raven.Server.Documents.Indexes.Static
{
    public class JavaScriptReduceOperation
    {
        public JavaScriptReduceOperation(ScriptFunctionInstance reduce, ScriptFunctionInstance key, Engine engine, JintPreventResolvingTasksReferenceResolver resolver)
        {
            Reduce = reduce;
            Key = key;
            Engine = engine;
            _resolver = resolver;
        }
        private readonly JsValue[] _oneItemArray = new JsValue[1];
        private readonly JsValue[] _threeItemsArray = new JsValue[3];
        private JintPreventResolvingTasksReferenceResolver _resolver;

        public IEnumerable IndexingFunction(IEnumerable<dynamic> items)
        {
            try
            {
                var map = Engine.Object.Construct(Arguments.Empty);
                foreach (var item in items)
                {
                    Engine.ResetCallStack();
                    Engine.ResetStatementsCount();
                    Engine.ResetTimeoutTicks();
                    if (JavaScriptIndexUtils.GetValue(Engine, item, out JsValue jsItem, true) == false)
                        continue;
                    _threeItemsArray[0] = map;
                    _threeItemsArray[1] = jsItem;
                    _threeItemsArray[2] = Key;
                    Engine.Invoke("groupItemsByKey", JsValue.Null, _threeItemsArray);
                    _resolver.ExplodeArgsOn(null, null);
                }

                foreach (var (name, prop) in map.GetOwnProperties())
                {
                    Engine.ResetCallStack();
                    Engine.ResetStatementsCount();
                    Engine.ResetTimeoutTicks();
                    _oneItemArray[0] = prop.Value;
                    var jsItem = Reduce.Call(JsValue.Null, _oneItemArray).AsObject();
                    yield return jsItem;
                    _resolver.ExplodeArgsOn(null, null);
                }
            }
            finally
            {
                _oneItemArray[0] = null;
                _threeItemsArray[0] = null;
                _threeItemsArray[1] = null;
                _threeItemsArray[2] = null;
            }
        }

        public Engine Engine { get; }

        public ScriptFunctionInstance Reduce { get; }
        public ScriptFunctionInstance Key { get; }

        internal string[] GetReduceFieldsNames()
        {
            var ast = Key.GetFunctionAst();
            var body = ast.Body.Body;
            if(body.Count != 1)
            {
                throw new InvalidOperationException($"Was requested to get reduce fields from a scripted function in an unexpected format, expected a single return statment got {body.Count}.");
            }
            var @params = ast.Params;
            if (@params.Count != 1)
            {
                throw new InvalidOperationException($"Was requested to get reduce fields from a scripted function in an unexpected format, expected a single argument but got {@params.Count}.");
            }
            if(@params[0] is Identifier == false)
            {
                throw new InvalidOperationException($"Was requested to get reduce fields from a scripted function in an unexpected format, expected a single argument of type 'Identifier' but got {@params[0].GetType().Name}.");
            }
            var actualBody = body[0];
            if(!(actualBody is ReturnStatement returnStatment) )
            {
                throw new InvalidOperationException($"Was requested to get reduce fields from a scripted function in an unexpected format, expected a single return statment got a statment of type {actualBody.GetType().Name}.");
            }
            if (!(returnStatment.Argument is ObjectExpression oe))
            {
                if(returnStatment.Argument is StaticMemberExpression sme && sme.Property is Identifier id)
                {
                    return new string[] {id.Name};
                }
                throw new InvalidOperationException($"Was requested to get reduce fields from a scripted function in an unexpected format, expected a single return object expression statment got a statment of type {actualBody.GetType().Name}.");
            }
            var fields = new List<string>();
            foreach (var prop in oe.Properties)
            {
                var fieldName = prop.Key.GetKey();
                fields.Add(fieldName);
            }
            return fields.ToArray();
        }
    }
}
