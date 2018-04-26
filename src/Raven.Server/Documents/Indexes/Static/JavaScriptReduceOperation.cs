using System;
using System.Collections;
using System.Collections.Generic;
using Jint;
using Jint.Native;
using Jint.Native.Function;
using Raven.Server.Documents.Patch;
using Raven.Server.Extensions;
using Esprima.Ast;

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
        private JintPreventResolvingTasksReferenceResolver _resolver;

        public IEnumerable IndexingFunction(IEnumerable<dynamic> items)
        {
            try
            {
                //TODO: 1. we need to create an array of the items and pass it once to the reduce function.
                //TODO: 2. we need to decide if we want to pass the actual key to the reduce function as it is tricky.
                foreach (var item in items)
                {
                    if (JavaScriptIndexUtils.GetValue(Engine, item, out JsValue jsItem, true) == false)
                        continue;
                    _oneItemArray[0] = jsItem;
                    jsItem = Reduce.Call(JsValue.Null, _oneItemArray);
                    yield return jsItem.AsObject();
                }
            }
            finally
            {
                _oneItemArray[0] = null;
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
