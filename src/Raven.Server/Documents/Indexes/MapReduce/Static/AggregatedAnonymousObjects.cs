using System;
using System.Collections.Generic;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Indexes.MapReduce.Static
{
    public class AggregatedAnonymousObjects : AggregationResult
    {
        protected Action<DynamicJsonValue> ModifyOutputToStore;

        private List<object> _outputs;
        private List<BlittableJsonReaderObject> _jsons;
        private IPropertyAccessor _propertyAccessor;
        private JsonOperationContext _indexContext;

        public AggregatedAnonymousObjects(List<object> results, IPropertyAccessor propertyAccessor, JsonOperationContext indexContext)
        {
            _outputs = results;
            _propertyAccessor = propertyAccessor;
            _jsons = new List<BlittableJsonReaderObject>(results.Count);
            _indexContext = indexContext;
        }

        public override int Count => _outputs.Count;

        public override IEnumerable<object> GetOutputs()
        {
            return _outputs;
        }

        public override IEnumerable<BlittableJsonReaderObject> GetOutputsToStore()
        {
            foreach (var output in _outputs)
            {
                var djv = new DynamicJsonValue();

                foreach (var property in _propertyAccessor.GetPropertiesInOrder(output))
                {
                    var value = property.Value;
                    djv[property.Key] = TypeConverter.ToBlittableSupportedType(value, context: _indexContext);
                }

                ModifyOutputToStore?.Invoke(djv);

                var item = _indexContext.ReadObject(djv, "map/reduce result to store");
                _jsons.Add(item);
                yield return item;
            }
        }

        public override void Dispose()
        {  
            for (int i = _jsons.Count - 1; i >= 0; i--)
            {
                _jsons[i].Dispose();
            }
            _jsons.Clear();
            //TODO: egor
            //if (JsOptions.EngineType == JavaScriptEngineType.V8)
            //{
            //    V8EngineEx.DisposeAndCollectGarbage(_outputs, "reduce");
            //}

            _outputs.Clear();

            GC.SuppressFinalize(this);
        }
    }
}
