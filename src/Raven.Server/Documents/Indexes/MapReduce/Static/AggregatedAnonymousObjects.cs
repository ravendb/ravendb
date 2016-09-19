using Sparrow.Json;
using System.Collections.Generic;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Indexes.MapReduce.Static
{
    public class AggregatedAnonymousObjects : AggregationResult
    {
        private readonly List<object> _outputs;
        private PropertyAccessor _propertyAccessor;
        private JsonOperationContext _indexContext;

        private BlittableJsonReaderObject _json;

        public AggregatedAnonymousObjects(List<object> results, PropertyAccessor propertyAccessor, TransactionOperationContext indexContext)
        {
            _outputs = results;
            _propertyAccessor = propertyAccessor;
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
                _json?.Dispose();

                var djv = new DynamicJsonValue();

                foreach (var property in _propertyAccessor.Properties)
                {
                    var value = property.Value.GetValue(output);
                    djv[property.Key] = TypeConverter.ToBlittableSupportedType(value, _indexContext);
                }

                _json = _indexContext.ReadObject(djv, "map/reduce result to store");

                yield return _json;
            }
        }

        public override void Dispose()
        {
            _json?.Dispose();
        }
    }
}
