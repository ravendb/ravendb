using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Esprima;
using Esprima.Ast;
using Jint;
using Jint.Native;
using Jint.Native.Function;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Patch;

namespace Raven.Server.Documents.Indexes.Static
{
    public class JavaScriptIndex : StaticIndexBase
    {
        public JavaScriptIndex(IndexDefinition definition)
        {
            _definitions = definition;
            var jint = new Engine();
            jint.Execute(Code);
            foreach (var map in definition.Maps)
            {
                jint.Execute(map);
                //ExtractFields(map);
            }
            // TODO: This code currently assume proper strucutre, but need to make sure
            // TODO: we get proper errors if the maps contains a value that is null, or a number, etc
            // TODO: That kind of error should throw, but we should be able to get clear error message and ensure that the
            // TODO: user has a proper way to recover from that

            var definitions = jint.GetValue("globalDefinition").AsObject();
            var maps = definitions.GetProperty("maps").Value.AsArray();
            var _collectionFunctions = new Dictionary<string, List<MapOperation>>();
            for (int i = 0; i < maps.GetLength(); i++)
            {
                var map = maps.Get(i.ToString()).AsObject();
                var mapCollection = map.Get("Collection").AsString();
                if (_collectionFunctions.TryGetValue(mapCollection, out var list) == false)
                {
                    list = new List<MapOperation>();
                    _collectionFunctions.Add(mapCollection, list);
                }
                var operation = new MapOperation();
                var mapChain = map.Get("Chain").AsArray();

                var chainLength = mapChain.GetLength();
                for (int j = 0; j < chainLength; j++)
                {
                    var method = mapChain.Get(j.ToString());
                    operation.Steps.Add(method.As<FunctionInstance>());
                }
                list.Add(operation);
            }

            var reduce = definitions.GetProperty("reduce").Value;
            if (reduce.IsNull() == false)
            {
                GroupByFields = new string[] { };
                Reduce = new JintReduceFuncWrapper(reduce.As<FunctionInstance>(), jint).IndexingFunction;
            }
            var fields = new HashSet<string>();
            foreach (var (key, val) in _collectionFunctions)
            {
                Maps.Add(key, val.Select(x => (IndexingFunc)new JintMapFuncWrapper(x, jint).IndexingFunction).ToList());
                /*foreach (var field in ExtractFields(val.LastOrDefault()?.LastOrDefault()))
                {
                    fields.Add(field);
                }*/
            }

            //Reduce = (IndexingFunc)JintReduceFuncWrapper;
            OutputFields = fields.ToArray();
            //TODO: we can extract the fields for the simple cases but for the general case it will require alot of effort.
            OutputFields = new string[]{};
            HasDynamicFields = true;
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
                Functions = operation.Steps;
                Engine = engine;
            }
            private readonly JsValue[] _oneItemArray = new JsValue[1];
            

            public IEnumerable IndexingFunction(IEnumerable<object> items)
            {
                try
                {
                    foreach (var item in items)
                    {
                        if (GetValue(Engine, item, out JsValue jsItem) == false)
                            continue;
                        var filtered = false;
                        foreach (var function in Functions)
                        {
                            _oneItemArray[0] = jsItem;
                            jsItem = function.Call(JsValue.Null, _oneItemArray);
                            //filter
                            if (jsItem.IsBoolean())
                            {
                                if (jsItem.AsBoolean() == false)
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
                finally
                {
                    _oneItemArray[0] = null;
                }
            }

            public List<FunctionInstance> Functions { get;}
            public Engine Engine { get; }
        }

        public static bool GetValue(Engine engine , dynamic item, out JsValue jsItem)
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

        public class MapOperation
        {
            public List<FunctionInstance> Steps = new List<FunctionInstance>();
        }
    }
}
