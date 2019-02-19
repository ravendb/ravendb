using System;
using System.Collections.Generic;
using System.Linq;
using Esprima.Ast;
using Jint;
using Jint.Native;
using Raven.Server.Documents.Patch;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static
{
    public static class JavaScriptIndexUtils
    {
        public static IEnumerable<ReturnStatement> GetReturnStatements(Statement stmt)
        {
            // here we only traverse the single statement, we don't try to traverse into
            // complex expression, etc. This is to avoid too much complexity such as:
            // return (function() { return { a: 1})();, etc. 
            switch (stmt?.Type)
            {
                case null:
                case Nodes.BreakStatement:
                case Nodes.DebuggerStatement:
                case Nodes.EmptyStatement:
                case Nodes.ContinueStatement:
                case Nodes.ThrowStatement:

                case Nodes.ExpressionStatement: // cannot contain return that we are interested in

                    return Enumerable.Empty<ReturnStatement>();

                case Nodes.BlockStatement:
                    return GetReturnStatements(((BlockStatement)stmt).Body);
                case Nodes.DoWhileStatement:
                    return GetReturnStatements(((DoWhileStatement)stmt).Body);
                case Nodes.ForStatement:
                    return GetReturnStatements(((ForStatement)stmt).Body);
                case Nodes.ForInStatement:
                    return GetReturnStatements(((ForInStatement)stmt).Body);

                case Nodes.IfStatement:
                    var ifStatement = ((IfStatement)stmt);
                    return GetReturnStatements(ifStatement.Consequent)
                        .Concat(GetReturnStatements(ifStatement.Alternate));

                case Nodes.LabeledStatement:
                    return GetReturnStatements(((LabeledStatement)stmt).Body);

                case Nodes.SwitchStatement:
                    return GetReturnStatements(((SwitchStatement)stmt).Cases.SelectMany(x => x.Consequent));

                case Nodes.TryStatement:
                    return GetReturnStatements(((TryStatement)stmt).Block);

                case Nodes.WhileStatement:
                    return GetReturnStatements(((WhileStatement)stmt).Body);


                case Nodes.WithStatement:
                    return GetReturnStatements(((WithStatement)stmt).Body);

                case Nodes.ForOfStatement:
                    return GetReturnStatements(((ForOfStatement)stmt).Body);

                case Nodes.ReturnStatement:
                    return new[] { (ReturnStatement)stmt };

                default:
                    return Enumerable.Empty<ReturnStatement>();
            }
        }

        private static IEnumerable<ReturnStatement> GetReturnStatements(IEnumerable<StatementListItem> items)
        {
            foreach (var item in items)
            {
                if (item is Statement nested)
                {
                    foreach (var returnStatement in GetReturnStatements(nested))
                    {
                        yield return returnStatement;
                    }
                }
            }
        }

        public static bool GetValue(Engine engine, object item, out JsValue jsItem, bool isMapReduce = false)
        {
            jsItem = null;
            if (!(item is DynamicBlittableJson dbj))
            {
                //This is the case for map-reduce
                if(item is BlittableJsonReaderObject bjr)
                {
                    jsItem = new BlittableObjectInstance(engine, null, bjr, null, null);
                    return true;
                }
                return false;
            }
            var id = dbj.GetId();
            if (isMapReduce == false && id == DynamicNullObject.Null)
                return false;
            jsItem = new BlittableObjectInstance(engine, null, dbj.BlittableJson, id, null);
            return true;
        }

        [ThreadStatic]
        private static JsValue[] _oneItemArray;

        public static object StringifyObject(JsValue jsValue)
        {
            if (_oneItemArray == null)
                _oneItemArray = new JsValue[1];
            _oneItemArray[0] = jsValue;
            try
            {
                // json string of the object
                return jsValue.AsObject().Engine.Json.Stringify(JsValue.Null, _oneItemArray);
            }
            finally
            {
                _oneItemArray[0] = null;
            }
        }
    }
}
