using System;
using Sparrow.Json;
using System.Collections.Generic;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Sparrow.Json.Parsing;
using Raven.Server.Utils;
using Raven.Client.ServerWide.JavaScript;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Patch.V8;

namespace Raven.Server.Documents.Indexes.MapReduce.Static
{
    public class AggregatedAnonymousObjects : AggregationResult
    {
        protected readonly IJavaScriptOptions JsOptions;
        protected Action<DynamicJsonValue> ModifyOutputToStore;

        private List<object> _outputs;
        private List<BlittableJsonReaderObject> _jsons;
        private IPropertyAccessor _propertyAccessor;
        private JsonOperationContext _indexContext;

        public AggregatedAnonymousObjects(IJavaScriptOptions jsOptions, List<object> results, IPropertyAccessor propertyAccessor, JsonOperationContext indexContext)
        {
            JsOptions = jsOptions;
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

            if (JsOptions.EngineType == JavaScriptEngineType.V8)
            {
                V8EngineEx.DisposeAndCollectGarbage(_outputs, "reduce");
            }

            _outputs.Clear();

            GC.SuppressFinalize(this);
        }
    }
}
