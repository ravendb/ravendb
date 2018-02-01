using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using Esprima;
using Esprima.Ast;
using Jint;
using Jint.Native;
using Jint.Native.Array;
using Jint.Native.Function;
using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Indexes.Static
{
    public class JavaScriptIndex : StaticIndexBase
    {
        private readonly Engine _jint;

        public JavaScriptIndex(IndexDefinition definition)
        {            
            _definitions = definition;
            _jint = new Engine();
            _jint.Execute(Code);
            foreach (var map in definition.Maps)
            {
                _jint.Execute(map);
                ExtractFields(map);
            }
            var definitions = _jint.GetValue("globalDefinition").AsObject();
            var maps = definitions.GetProperty("maps").Value.AsArray();
            var mapLength = maps.GetLength();
            _collectionFunctions = new Dictionary<string, List<List<JsValue>>>();
            for (int i = 0; i < mapLength; i++)
            {
                var map = maps.Get(i.ToString()).AsObject();
                var mapCollection = map.Get("Collection").AsString();
                if (_collectionFunctions.TryGetValue(mapCollection, out var list) == false)
                {
                    list = new List<List<JsValue>>();
                    _collectionFunctions.Add(mapCollection, list);
                }
                var chainList = new List<JsValue>();
                var mapChain = map.Get("Chain").AsArray();

                var chainLength = mapChain.GetLength();
                for (int j = 0; j < chainLength; j++)
                {
                    var method = mapChain.Get(j.ToString());
                    chainList.Add(method);
                }
                list.Add(chainList);
            }
            var fields = new HashSet<string>();
            foreach (var (key, val) in _collectionFunctions)
            {
                Maps.Add(key, val.Select(x => (IndexingFunc)(enm => MyWrapper(x, enm))).ToList());
                foreach (var field in ExtractFields(val.LastOrDefault()?.LastOrDefault()))
                {
                    fields.Add(field);
                }
            }
            OutputFields = fields.ToArray();
            //TODO: we can extract the fields for the simple cases but for the general case it will require alot of effort.
            OutputFields = new string[]{"Name","Count"};
        }

        private static void ExtractFields(string map)
        {
            JavaScriptParser parser = new JavaScriptParser(map);
            var res = parser.ParseProgram();
            foreach (var statement in res.Body)
            {
                var s = statement.As<Statement>();
                switch (s.Type)
                {
                    case Nodes.AssignmentExpression:
                        break;
                    case Nodes.ArrayExpression:
                        break;
                    case Nodes.BlockStatement:
                        break;
                    case Nodes.BinaryExpression:
                        break;
                    case Nodes.BreakStatement:
                        break;
                    case Nodes.CallExpression:
                        break;
                    case Nodes.CatchClause:
                        break;
                    case Nodes.ConditionalExpression:
                        break;
                    case Nodes.ContinueStatement:
                        break;
                    case Nodes.DoWhileStatement:
                        break;
                    case Nodes.DebuggerStatement:
                        break;
                    case Nodes.EmptyStatement:
                        break;
                    case Nodes.ExpressionStatement:
                        break;
                    case Nodes.ForStatement:
                        break;
                    case Nodes.ForInStatement:
                        break;
                    case Nodes.FunctionDeclaration:
                        break;
                    case Nodes.FunctionExpression:
                        break;
                    case Nodes.Identifier:
                        break;
                    case Nodes.IfStatement:
                        break;
                    case Nodes.Literal:
                        break;
                    case Nodes.LabeledStatement:
                        break;
                    case Nodes.LogicalExpression:
                        break;
                    case Nodes.MemberExpression:
                        break;
                    case Nodes.NewExpression:
                        break;
                    case Nodes.ObjectExpression:
                        break;
                    case Nodes.Program:
                        break;
                    case Nodes.Property:
                        break;
                    case Nodes.RestElement:
                        break;
                    case Nodes.ReturnStatement:
                        break;
                    case Nodes.SequenceExpression:
                        break;
                    case Nodes.SwitchStatement:
                        break;
                    case Nodes.SwitchCase:
                        break;
                    case Nodes.TemplateElement:
                        break;
                    case Nodes.TemplateLiteral:
                        break;
                    case Nodes.ThisExpression:
                        break;
                    case Nodes.ThrowStatement:
                        break;
                    case Nodes.TryStatement:
                        break;
                    case Nodes.UnaryExpression:
                        break;
                    case Nodes.UpdateExpression:
                        break;
                    case Nodes.VariableDeclaration:
                        break;
                    case Nodes.VariableDeclarator:
                        break;
                    case Nodes.WhileStatement:
                        break;
                    case Nodes.WithStatement:
                        break;
                    case Nodes.ArrayPattern:
                        break;
                    case Nodes.AssignmentPattern:
                        break;
                    case Nodes.SpreadElement:
                        break;
                    case Nodes.ObjectPattern:
                        break;
                    case Nodes.ArrowParameterPlaceHolder:
                        break;
                    case Nodes.MetaProperty:
                        break;
                    case Nodes.Super:
                        break;
                    case Nodes.TaggedTemplateExpression:
                        break;
                    case Nodes.YieldExpression:
                        break;
                    case Nodes.ArrowFunctionExpression:
                        break;
                    case Nodes.ClassBody:
                        break;
                    case Nodes.ClassDeclaration:
                        break;
                    case Nodes.ForOfStatement:
                        break;
                    case Nodes.MethodDefinition:
                        break;
                    case Nodes.ImportSpecifier:
                        break;
                    case Nodes.ImportDefaultSpecifier:
                        break;
                    case Nodes.ImportNamespaceSpecifier:
                        break;
                    case Nodes.ImportDeclaration:
                        break;
                    case Nodes.ExportSpecifier:
                        break;
                    case Nodes.ExportNamedDeclaration:
                        break;
                    case Nodes.ExportAllDeclaration:
                        break;
                    case Nodes.ExportDefaultDeclaration:
                        break;
                    case Nodes.ClassExpression:
                        break;
                }
            }
        }

        private IEnumerable<string> ExtractFields(JsValue map)
        {
            var bluh = map.ToString();
            
            if (map == null)
                yield break;
        }

        private IEnumerable MyWrapper(List<JsValue> jsValues, IEnumerable<dynamic> items)
        {
            foreach (var item in items)
            {
                JsValue jsItem = JsValue.FromObject(_jint, item);
                var filtered = false;
                foreach (var function in jsValues)
                {
                    jsItem = function.Invoke(jsItem);
                    //filter
                    if (jsItem.IsBoolean())
                    {
                        if (jsItem.AsBoolean())
                        {
                            filtered = true;
                            break;
                        }
                    }
                }
                if (filtered == false)
                {
                    //Fanout
                    if (jsItem.IsArray())
                    {
                        var array = jsItem.AsArray();
                        var len = array.GetLength();
                        for (var i = 0; i < len; i++)
                        {
                            yield return array.Get(i.ToString());
                        }
                    }
                    else
                        yield return jsItem.AsObject();
                }
            }
        }

        private static string Code = @"
var globalDefinition =
{
    maps: [],
    reduce: null
}

function collection(name) {

    var map = {
        Collection: name,
        Chain: []                
    };    
    globalDefinition.maps.push(map);
    map.map = map.filter = function(lambda) { map.Chain.push(lambda); return map; };    
    return map;
}

function groupBy(lambda) {
    var reduce = globalDefinition.reduce = { _data: {} };
    reduce.key = lambda;
    reduce.aggregate = function(reduceFunction){reduce.aggregateBy = reduceFunction;}
    return reduce;
}";

        private IndexDefinition _definitions;
        private Dictionary<string, List<List<JsValue>>> _collectionFunctions;

        //       public IEnumerable IndexingFunc(IEnumerable<dynamic> items);
    }
}
