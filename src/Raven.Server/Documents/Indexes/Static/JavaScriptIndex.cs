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
using Raven.Server.Config;
using Raven.Server.Documents.Patch;

namespace Raven.Server.Documents.Indexes.Static
{
    public class JavaScriptIndex : StaticIndexBase
    {
        private static readonly string GlobalDefinitions = "globalDefinition";
        private static readonly string MapsProperty = "maps";
        private static readonly string CollectionProperty = "collection";
        private static readonly string MethodProperty = "method";
        private static readonly string MoreArgsProperty = "moreArgs";
        private static readonly string ReduceProperty = "reduce";
        private static readonly string AggregateByProperty = "aggregateBy";
        private static readonly string KeyProperty = "key";

        private JintPreventResolvingTasksReferenceResolver _resolver;
        public JavaScriptIndex(IndexDefinition definition, RavenConfiguration configuration)
        {
            _definitions = definition;
            _resolver = new JintPreventResolvingTasksReferenceResolver();
            // we create the Jint instance directly instead of using SingleRun
            // because the index is single threaded and long lived
            _engine = new Engine(options =>
            {
                options.LimitRecursion(64)
                    .SetReferencesResolver(_resolver)
                    .MaxStatements(configuration.Indexing.MaxStepsForScript)
                    .Strict()
                    .AddObjectConverter(new JintGuidConverter())
                    .AddObjectConverter(new JintStringConverter())
                    .AddObjectConverter(new JintEnumConverter())
                    .AddObjectConverter(new JintDateTimeConverter())
                    .AddObjectConverter(new JintTimeSpanConverter())
                    .LocalTimeZone(TimeZoneInfo.Utc);

            });
            _engine.SetValue("load", new ClrFunctionInstance(_engine, LoadDocument));
            _engine.Execute(Code);

            if (definition.AdditionalSources != null)
            {
                foreach (var script in definition.AdditionalSources.Values)
                {
                    _engine.Execute(script);
                }
            }
            var mapList = definition.Maps.ToList();

            for (var i =0; i < mapList.Count; i++)
            { 
                _engine.Execute(mapList[i]);
            }

            if (definition.Reduce != null)
                _engine.Execute(definition.Reduce);            

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
            var collectionFunctions = new Dictionary<string, List<JavaScriptMapOperation>>();
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
                    list = new List<JavaScriptMapOperation>();
                    collectionFunctions.Add(mapCollection, list);
                }
                if(map.HasProperty(MethodProperty) == false)
                    ThrowIndexCreationException($"map function #{i} is missing its {MethodProperty} property");
                var funcInstance = map.Get(MethodProperty).As<FunctionInstance>();
                if(funcInstance == null)
                    ThrowIndexCreationException($"map function #{i} {MethodProperty} property isn't a 'FunctionInstance'");                
                var operation = new JavaScriptMapOperation(_engine,_resolver)
                {
                    MapFunc = funcInstance,
                    IndexName = _definitions.Name,
                    Configuration = configuration,
                    MapString = mapList[i]
                };
                if (map.HasOwnProperty(MoreArgsProperty))
                {
                    var moreArgsObj = map.Get(MoreArgsProperty);
                    if (moreArgsObj.IsArray())
                    {
                        var array = moreArgsObj.AsArray();
                        if (array.GetLength() > 0)
                        {
                            operation.MoreArguments = array;
                        }
                    }
                }
                operation.Analyze(_engine);
                if (ReferencedCollections.TryGetValue(mapCollection, out var collectionNames) == false)
                {
                    collectionNames = new HashSet<CollectionName>();
                    ReferencedCollections.Add(mapCollection, collectionNames);
                }
                collectionNames.UnionWith(operation.ReferencedCollection);

                list.Add(operation);
            }

            var reduceObj = definitions.GetProperty(ReduceProperty)?.Value;
            if (reduceObj != null && reduceObj.IsObject())
            {
                var reduceAsObj = reduceObj.AsObject();
                var groupByKey = reduceAsObj.GetProperty(KeyProperty).Value.As<ScriptFunctionInstance>();
                var reduce = reduceAsObj.GetProperty(AggregateByProperty).Value.As<ScriptFunctionInstance>();
                ReduceOperation = new JavaScriptReduceOperation(reduce, groupByKey, _engine, _resolver) { ReduceString = definition.Reduce};
                GroupByFields = ReduceOperation.GetReduceFieldsNames();
                Reduce = ReduceOperation.IndexingFunction;
            }
            var fields = new HashSet<string>();
            HasDynamicFields = false;
            foreach (var (key, val) in collectionFunctions)
            {
                Maps.Add(key, val.Select(x => (IndexingFunc)x.IndexingFunction).ToList());

                //TODO: Validation of matches fields between group by / collections / etc
                foreach (var operation in val)
                {
                    HasDynamicFields |= operation.HasDynamicReturns;
                    fields.UnionWith(operation.Fields);
                    foreach ((var k, var v) in operation.FieldOptions)
                    {
                        _definitions.Fields.Add(k,v);
                    }
                    
                }
            }
            if (definition.Fields != null)
            {
                foreach (var item in definition.Fields)
                {
                    fields.Add(item.Key);
                }
            }
            OutputFields = fields.ToArray();
        }

        private void ThrowIndexCreationException(string message)
        {
            throw new IndexCreationException($"Javascript index {_definitions.Name} {message}");
        }

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
            if (JavaScriptIndexUtils.GetValue(_engine, doc, out var item))
                return item;

            return JsValue.Undefined;
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
        method: lambda,
        moreArgs: Array.prototype.slice.call(arguments, 2)
    };    
    globalDefinition.maps.push(map);
}

function groupBy(lambda) {
    var reduce = globalDefinition.reduce = { };
    reduce.key = lambda;
 
    reduce.aggregate = function(reduceFunction){
        reduce.aggregateBy = reduceFunction;
    }
    return reduce;
}";

        private IndexDefinition _definitions;
        private Engine _engine;

        public JavaScriptReduceOperation ReduceOperation { get; }
    }
}
