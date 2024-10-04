using System;
using System.Collections.Generic;
using System.Linq;
using Acornima.Ast;
using Jint;
using Jint.Native;
using Jint.Native.Json;
using Raven.Client;
using Raven.Server.Documents.Indexes.Static.Counters;
using Raven.Server.Documents.Indexes.Static.TimeSeries;
using Raven.Server.Documents.Patch;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static
{
    public static class JavaScriptIndexUtils
    {
        public static IEnumerable<ReturnStatement> GetReturnStatements(IFunction function)
        {
            if (function is ArrowFunctionExpression arrowFunction && arrowFunction.Body is ObjectExpression objectExpression)
            {
                // looks like we have following case:
                // x => ({ Name: x.Name })
                // wrap with return statement and we're done
                return new[] { new ReturnStatement(objectExpression) };
            }

            return GetReturnStatements(function.Body);
        }
        
        public static IEnumerable<ReturnStatement> GetReturnStatements(Node stmt)
        {
            // here we only traverse the single statement, we don't try to traverse into
            // complex expression, etc. This is to avoid too much complexity such as:
            // return (function() { return { a: 1})();, etc.
            switch (stmt?.Type)
            {
                case null:
                case NodeType.BreakStatement:
                case NodeType.DebuggerStatement:
                case NodeType.EmptyStatement:
                case NodeType.ContinueStatement:
                case NodeType.ThrowStatement:

                case NodeType.ExpressionStatement: // cannot contain return that we are interested in

                    return Enumerable.Empty<ReturnStatement>();

                case NodeType.BlockStatement:
                    return GetReturnStatements(((BlockStatement)stmt).Body);
                case NodeType.DoWhileStatement:
                    return GetReturnStatements(((DoWhileStatement)stmt).Body);
                case NodeType.ForStatement:
                    return GetReturnStatements(((ForStatement)stmt).Body);
                case NodeType.ForInStatement:
                    return GetReturnStatements(((ForInStatement)stmt).Body);

                case NodeType.IfStatement:
                    var ifStatement = ((IfStatement)stmt);
                    return GetReturnStatements(ifStatement.Consequent)
                        .Concat(GetReturnStatements(ifStatement.Alternate));

                case NodeType.LabeledStatement:
                    return GetReturnStatements(((LabeledStatement)stmt).Body);

                case NodeType.SwitchStatement:
                    return GetReturnStatements(((SwitchStatement)stmt).Cases.SelectMany(x => x.Consequent));

                case NodeType.TryStatement:
                    return GetReturnStatements(((TryStatement)stmt).Block);

                case NodeType.WhileStatement:
                    return GetReturnStatements(((WhileStatement)stmt).Body);

                case NodeType.WithStatement:
                    return GetReturnStatements(((WithStatement)stmt).Body);

                case NodeType.ForOfStatement:
                    return GetReturnStatements(((ForOfStatement)stmt).Body);

                case NodeType.ReturnStatement:
                    return new[] { (ReturnStatement)stmt };

                default:
                    return Enumerable.Empty<ReturnStatement>();
            }
        }

        private static IEnumerable<ReturnStatement> GetReturnStatements(IEnumerable<StatementOrExpression> items)
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
            string changeVector = null;
            DateTime? lastModified = null;

            switch (item)
            {
                case DynamicBlittableJson dbj:
                    var id = dbj.GetId();
                    if (isMapReduce == false && id == DynamicNullObject.Null)
                        return false;

                    dbj.EnsureMetadata();

                    if (dbj.TryGetDocument(out var doc))
                    {
                        jsItem = new BlittableObjectInstance(engine, null, dbj.BlittableJson, doc);
                    }
                    else
                    {
                        if (dbj[Constants.Documents.Metadata.LastModified] is DateTime lm)
                            lastModified = lm;

                        if (dbj[Constants.Documents.Metadata.ChangeVector] is string cv)
                            changeVector = cv;

                        jsItem = new BlittableObjectInstance(engine, null, dbj.BlittableJson, id, lastModified, changeVector);
                    }

                    return true;

                case DynamicTimeSeriesSegment dtss:
                    jsItem = new TimeSeriesSegmentObjectInstance(engine, dtss);
                    return true;

                case DynamicCounterEntry dce:
                    jsItem = new CounterEntryObjectInstance(engine, dce);
                    return true;

                case BlittableJsonReaderObject bjro:
                    //This is the case for map-reduce
                    jsItem = new BlittableObjectInstance(engine, null, bjro, null, null, null);
                    return true;
            }

            return false;
        }

        public static object StringifyObject(JsValue jsValue)
        {
            // json string of the object
            Engine engine = jsValue.AsObject().Engine;
            var serializer = new JsonSerializer(engine);
            return serializer.Serialize(jsValue);
        }
    }
}
