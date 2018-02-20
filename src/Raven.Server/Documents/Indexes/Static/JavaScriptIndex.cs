using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Esprima;
using Esprima.Ast;
using Jint;
using Jint.Native;
using Jint.Native.Function;
using Jint.Runtime.Interop;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Patch;

namespace Raven.Server.Documents.Indexes.Static
{
    public class JavaScriptIndex : StaticIndexBase
    {
        private static readonly string GlobalDefinitions = "globalDefinition";
        private static readonly string MapsProperty = "maps";
        private static readonly string CollectionProperty = "collection";
        private static readonly string MethodProperty = "method";
        private static readonly string ReduceProperty = "reduce";

        public JavaScriptIndex(IndexDefinition definition)
        {
            _definitions = definition;
            // we create the Jint instance directly instead of using SingleRun
            // because the index is single threaded and long lived
            _engine = new Engine();
            _engine.SetValue("load", new ClrFunctionInstance(_engine, LoadDocument));
            _engine.Execute(Code);
            foreach (var map in definition.Maps)
            {
                _engine.Execute(map);
                //ExtractFields(map);
            }
            // TODO: This code currently assume proper strucutre, but need to make sure
            // TODO: we get proper errors if the maps contains a value that is null, or a number, etc
            // TODO: That kind of error should throw, but we should be able to get clear error message and ensure that the
            // TODO: user has a proper way to recover from that

            var definitionsObj = _engine.GetValue(GlobalDefinitions);
            if(definitionsObj.IsNull() || definitionsObj.IsUndefined() || definitionsObj.IsObject() == false)
                ThrowIndexCreationException($"is missing its '{GlobalDefinitions}' global variable, are you modifying it in your script?");
            var definitions = definitionsObj.AsObject();
            if(definitions.HasProperty(MapsProperty) == false)
                ThrowIndexCreationException("is missing its 'globalDefinition.maps' property, are you modifying it in your script?");
            var mapsArray = definitions.GetProperty(MapsProperty).Value;
            if (mapsArray.IsNull() || mapsArray.IsUndefined() || mapsArray.IsArray() == false)
                ThrowIndexCreationException($"doesn't contain any map function or '{GlobalDefinitions}.{Maps}' was modified in the script");
            var maps = mapsArray.AsArray();
            var collectionFunctions = new Dictionary<string, List<MapOperation>>();
            for (int i = 0; i < maps.GetLength(); i++)
            {
                var mapObj = maps.Get(i.ToString());
                if(mapObj.IsNull() || mapObj.IsUndefined() || mapObj.IsObject() == false)
                    ThrowIndexCreationException($"map function #{i} is not a valid object");
                var map = mapObj.AsObject();
                if(map.HasProperty(CollectionProperty) == false)
                    ThrowIndexCreationException($"map function #{i} is missing a collection name");
                var mapCollectionStr = map.Get(CollectionProperty);                    
                if (mapCollectionStr.IsString() == false)
                    ThrowIndexCreationException($"map function #{i} collection name isn't a string");
                var mapCollection = mapCollectionStr.AsString();
                if (collectionFunctions.TryGetValue(mapCollection, out var list) == false)
                {
                    list = new List<MapOperation>();
                    collectionFunctions.Add(mapCollection, list);
                }
                if(map.HasProperty(MethodProperty) == false)
                    ThrowIndexCreationException($"map function #{i} is missing its {MethodProperty} property");
                var funcInstance = map.Get(MethodProperty).As<FunctionInstance>();
                if(funcInstance == null)
                    ThrowIndexCreationException($"map function #{i} {MethodProperty} property isn't a 'FunctionInstance'");
                var operation = new MapOperation
                {
                    MapFunc = funcInstance,
                    IndexName = _definitions.Name
                };
                operation.Analyze();
                if (ReferencedCollections.TryGetValue(mapCollection, out var collectionNames) == false)
                {
                    collectionNames = new HashSet<CollectionName>();
                    ReferencedCollections.Add(mapCollection, collectionNames);
                }
                collectionNames.UnionWith(operation.ReferencedCollection);

                list.Add(operation);
            }
            var reduce = definitions.GetProperty(ReduceProperty).Value.As<FunctionInstance>();
            if(reduce != null)
            {
                GroupByFields = new string[] { };
                Reduce = new JintReduceFuncWrapper(reduce, _engine).IndexingFunction;
            }
            var fields = new HashSet<string>();
            HasDynamicFields = false;
            foreach (var (key, val) in collectionFunctions)
            {
                Maps.Add(key, val.Select(x => (IndexingFunc)new JintMapFuncWrapper(x, _engine).IndexingFunction).ToList());

                //TODO: Validation of matches fields between group by / collections / etc
                foreach (var operation in val)
                {
                    HasDynamicFields |= operation.HasDynamicReturns;
                    fields.UnionWith(operation.Fields); 
                }
            }
            OutputFields = fields.ToArray();
        }

        private void ThrowIndexCreationException(string message)
        {
            throw new IndexCreationException($"Javascript index {_definitions.Name} {message}");
        }

        //TODO: We need to calculate the refrenced collections for this to work.
        private JsValue LoadDocument(JsValue self, JsValue[] args)
        {
            if (args.Length != 2)
            {
                throw new ArgumentException("The load(id, collection) method expects two arguments, but got: " + args.Length);
            }

            if (args[0].IsNull() || args[0].IsUndefined())
                return JsValue.Undefined;

            if (args[0].IsString() == false ||
                args[1].IsString() == false)
            {
                throw new ArgumentException($"The load(id, collection) method expects two string arguments, but got: load({args[0]}, {args[1]})");
            }

            object doc =  CurrentIndexingScope.Current.LoadDocument(null, args[0].AsString(), args[1].AsString());
            if (GetValue(_engine, doc, out var item))
                return item;

            return JsValue.Undefined;
        }
        //TODO: Implement a visitor to the full AST structure
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

        private class JintReduceFuncWrapper
        {
            public JintReduceFuncWrapper(FunctionInstance reduce, Engine engine)
            {
                Reduce = reduce;
                Engine = engine;
            }
            private readonly JsValue[] _oneItemArray = new JsValue[1];

            public IEnumerable IndexingFunction(IEnumerable<dynamic> items)
            {
                try
                {
                    foreach (var item in items)
                    {
                        if (GetValue(Engine, item, out JsValue jsItem) == false)
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

            public FunctionInstance Reduce { get; }
        }

        private class JintMapFuncWrapper
        {
            public JintMapFuncWrapper(MapOperation operation, Engine engine)
            {
                _operation = operation;
                _engine = engine;
            }
            private readonly JsValue[] _oneItemArray = new JsValue[1];

            public IEnumerable IndexingFunction(IEnumerable<object> items)
            {
                try
                {
                    foreach (var item in items)
                    {
                        if (GetValue(_engine, item, out JsValue jsItem) == false)
                            continue;
                        {
                            _oneItemArray[0] = jsItem;
                            jsItem = _operation.MapFunc.Call(JsValue.Null, _oneItemArray);
                            if (jsItem.IsArray())
                            {
                                var array = jsItem.AsArray();
                                var len = array.GetLength();
                                for (var i = 0; i < len; i++)
                                {
                                    yield return array.Get(i.ToString());
                                }
                            }
                            else if (jsItem.IsObject())
                            {
                                yield return jsItem.AsObject();
                            }
                            // we ignore everything else by design, we support only
                            // objects and arrays, anything else is discarded
                        }

                    }
                }
                finally
                {
                    _oneItemArray[0] = null;
                }
            }

            private readonly MapOperation _operation;
            private readonly Engine _engine;
        }

        public static bool GetValue(Engine engine, object item, out JsValue jsItem)
        {
            jsItem = null;
            if (!(item is DynamicBlittableJson dbj))
                return false;
            var id = dbj.GetId();
            if (id == DynamicNullObject.Null)
                return false;
            jsItem = new BlittableObjectInstance(engine, null, dbj.BlittableJson, id, null);
            return true;
        }

        private static string Code = @"
var globalDefinition =
{
    maps: [],
    reduce: null
}

function map(name, lambda) {

    var map = {
        collection: name,
        method: lambda                
    };    
    globalDefinition.maps.push(map);
}

function groupBy(lambda) {
    var reduce = globalDefinition.reduce = { };
    reduce.key = lambda;
    reduce.aggregate = function(reduceFunction){reduce.aggregateBy = reduceFunction;}
    return reduce;
}";

        private IndexDefinition _definitions;
        private Engine _engine;

        public class MapOperation
        {
            public FunctionInstance MapFunc;

            public bool HasDynamicReturns;

            public HashSet<string> Fields = new HashSet<string>();
            public string IndexName { get; set; }

            public void Analyze()
            {
                //TODO: expose this in Jint directly instead of reflection
                //TODO: we should calculate the refrenced collection in this method
                var funcDeclField = typeof(ScriptFunctionInstance).GetField("_functionDeclaration", BindingFlags.Instance | BindingFlags.NonPublic);

                HasDynamicReturns = false;
                               
                if (!(MapFunc is ScriptFunctionInstance sfi))
                    return;

                var theFuncAst = (IFunction)funcDeclField.GetValue(sfi);

                var loadSearcher = new EsprimaReferencedCollectionVisitor();
                loadSearcher.VisitFunctionExpression(theFuncAst);
                ReferencedCollection.UnionWith(loadSearcher.ReferencedCollection);

                foreach (var returnStatement in GetReturnStatements(theFuncAst.Body))
                {
                    if (returnStatement.Argument == null) // return;
                        continue;

                    if (!(returnStatement.Argument is ObjectExpression oe))
                    {
                        HasDynamicReturns = true;
                        continue;
                    }
                    //If we got here we must validate that all return statments have the same structure.
                    //Having zero fields means its the first return statments we encounter that has a structure.
                    if (Fields.Count == 0)
                    {
                        foreach (var prop in oe.Properties)
                        {
                            Fields.Add(prop.Key.GetKey());
                        }
                    }
                    else if(CompareFields(oe) == false)
                    {
                        throw new InvalidOperationException($"Index {IndexName} contains diffrent return structure from different code paths," +
                                                            $" expected properties: {string.Join(", ", Fields)} but also got:{string.Join(", ", oe.Properties.Select(x => x.Key.GetKey()))}");
                    }
                }
            }

            public HashSet<CollectionName> ReferencedCollection { get; set; } = new HashSet<CollectionName>();

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
}
