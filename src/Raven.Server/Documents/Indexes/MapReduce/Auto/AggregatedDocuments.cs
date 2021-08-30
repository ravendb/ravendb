using System;
using Sparrow.Json;
using System.Collections.Generic;

namespace Raven.Server.Documents.Indexes.MapReduce.Auto
{
    public class AggregatedDocuments : AggregationResult
    {
        private bool _disposed = false;

        private List<Document> _outputs;

        public AggregatedDocuments(List<Document> results)
        {
            _outputs = results;
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
                yield return output.Data;
            }
        }

        ~AggregatedDocuments()
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
                for (int i = _outputs.Count - 1; i >= 0; i--)
                {
                    _outputs[i].Data.Dispose();
                }
                _outputs.Clear();
                _outputs = null;

                GC.SuppressFinalize(this);
            }

            // releasing unmanaged resources
            // ...

            _disposed = true;
        }
    }

}
