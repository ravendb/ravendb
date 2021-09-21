using System;
using Sparrow.Json;
using System.Collections.Generic;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Sparrow.Json.Parsing;
using Raven.Server.Utils;
using V8.Net;
using Raven.Server.Documents.Patch;

namespace Raven.Server.Documents.Indexes.MapReduce.Static
{
    public class AggregatedAnonymousObjects : AggregationResult
    {
        private bool _disposed = false;

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

                var item = _indexContext.ReadObject(djv, "map/reduce result to store");
                _jsons.Add(item);
                yield return item;
            }
        }

        ~AggregatedAnonymousObjects()
        {            
            Dispose(false);
        }


        public override void Dispose()
        {  
            Dispose(true);
        }

        public void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            if (disposing) {
                // releasing managed resources
                for (int i = _jsons.Count - 1; i >= 0; i--)
                {
                    _jsons[i].Dispose();
                }
                _jsons.Clear();
                _jsons = null;

                for (int i = _outputs.Count - 1; i >= 0; i--)
                {
                    V8Engine engine = null;
                    if (_outputs[i] is InternalHandle h) {
                        if (engine == null)
                            engine = h.Engine;
                        h.Dispose();
                    }
                    engine?.ForceV8GarbageCollection();
                }
                _outputs.Clear();
                _outputs = null;

                _propertyAccessor = null;
                _indexContext = null;

                GC.SuppressFinalize(this);
            }

            // releasing unmanaged resources
            // ...

            _disposed = true;
        }
    }
}
