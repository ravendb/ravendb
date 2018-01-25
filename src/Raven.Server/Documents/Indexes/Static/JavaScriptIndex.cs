using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using Jint;
using Jint.Native;
using Jint.Native.Array;
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
            foreach (var (key, val) in _collectionFunctions)
            {
                Maps.Add(key, val.Select(x => (IndexingFunc)(enm => MyWrapper(x, enm))).ToList());
            }
            

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
