using System;
using System.Collections.Generic;
using System.IO;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes
{
    public sealed class IndexFieldsPersistence
    {
        private bool _initialized;

        private readonly Index _index;

        private bool _supportsTimeFields;
        private HashSet<string> _timeFields;
        private HashSet<string> _timeFieldsToWrite;
        private Dictionary<string, int> _vectorFieldsDimensions;
        private Dictionary<string, int> _vectorFieldsDimensionsToWrite;

        public IndexFieldsPersistence(Index index)
        {
            _index = index ?? throw new ArgumentNullException(nameof(index));
            _supportsTimeFields = index.Definition.Version >= IndexDefinitionBaseServerSide.IndexVersion.TimeTicks;
        }

        internal void Initialize()
        {
            if (_initialized)
                throw new InvalidOperationException();

            _initialized = true;

            if (_supportsTimeFields)
                _timeFields = _index._indexStorage.ReadIndexTimeFields();
            
            _vectorFieldsDimensions = _index._indexStorage.ReadVectorDimensions();
            
            foreach (var indexField in _index.Definition.IndexFields.Values)
            {
                var fieldDimensions = indexField.Vector?.Dimensions;
                
                if (fieldDimensions is not null)
                {
                    var dimensionsToWrite = indexField.Vector.DestinationEmbeddingType switch
                    {
                        // In case of VectorEmbeddingType.Single the embedding length is multiplied by 4 (for every float we have 4 bytes), so to match this behavior we 
                        // have to multiply the length here
                        VectorEmbeddingType.Single => fieldDimensions.Value * sizeof(float),
                        VectorEmbeddingType.Int8 => fieldDimensions.Value,
                        // We don't restore original number of binary vector dimensions, so this value is not relevant 
                        VectorEmbeddingType.Binary => fieldDimensions.Value,
                        _ => throw new InvalidDataException($"Unexpected embedding type - {indexField.Vector.DestinationEmbeddingType}.")
                    };

                    _vectorFieldsDimensions.TryAdd(indexField.Name, dimensionsToWrite);
                }
            }
        }

        internal void MarkHasTimeValue(string fieldName)
        {
            if (_supportsTimeFields == false)
                return;

            if (_timeFields.Contains(fieldName))
                return;

            if (_timeFieldsToWrite == null)
                _timeFieldsToWrite = new HashSet<string>();

            _timeFieldsToWrite.Add(fieldName);
        }

        internal bool HasTimeValues(string fieldName)
        {
            return _supportsTimeFields && _timeFields.Contains(fieldName);
        }

        internal void SetFieldEmbeddingDimension(string fieldName, int dimensions, VectorEmbeddingType destinationEmbeddingType)
        {
            _vectorFieldsDimensionsToWrite ??= new Dictionary<string, int>();
            
            if (_vectorFieldsDimensions.TryGetValue(fieldName, out int storedDimensions) || _vectorFieldsDimensionsToWrite.TryGetValue(fieldName, out storedDimensions))
            {
                if (storedDimensions == dimensions)
                    return;
                
                if (destinationEmbeddingType == VectorEmbeddingType.Binary)
                    throw new InvalidDataException($"Field {fieldName} contains embeddings with different number of dimensions.");

                // Because dimensions number we get as a parameter is a length of a vector after the quantization and cast to bytes, we have to restore original 
                // length for exception message
                var (originalInputDimensions, originalDimensions) = destinationEmbeddingType switch
                {
                    VectorEmbeddingType.Single => (storedDimensions / sizeof(float), dimensions / sizeof(float)),
                    VectorEmbeddingType.Int8 => (storedDimensions, dimensions),
                    _ => throw new InvalidDataException($"Unexpected embedding type - {destinationEmbeddingType}.")
                };

                throw new InvalidDataException($"Attempted to index embedding with {originalDimensions} dimensions, but field {fieldName} already contains indexed embedding with {originalInputDimensions} dimensions, or was explicitly configured for embeddings with {originalInputDimensions} dimensions.");
            }
            
            _vectorFieldsDimensionsToWrite.Add(fieldName, dimensions);
        }
        
        internal void Persist(TransactionOperationContext indexContext)
        {
            if (_timeFieldsToWrite == null && _vectorFieldsDimensionsToWrite == null)
                return;
            
            if (_timeFieldsToWrite != null)
                _index._indexStorage.WriteIndexTimeFields(indexContext.Transaction, _timeFieldsToWrite);
            
            if (_vectorFieldsDimensionsToWrite != null)
                IndexStorage.WriteVectorDimensions(indexContext.Transaction, _vectorFieldsDimensionsToWrite);

            indexContext.Transaction.InnerTransaction.LowLevelTransaction.BeforeCommitFinalization += _ =>
            {
                if (_timeFieldsToWrite != null)
                {
                    var timeFields = new HashSet<string>(_timeFields);
                    foreach (var fieldName in _timeFieldsToWrite)
                        timeFields.Add(fieldName);

                    _timeFields = timeFields;
                    _timeFieldsToWrite = null;
                }

                if (_vectorFieldsDimensionsToWrite != null)
                {
                    var vectorFieldsDimensions = new Dictionary<string, int>(_vectorFieldsDimensions);
                    foreach (var fieldDimension in _vectorFieldsDimensionsToWrite)
                        vectorFieldsDimensions.Add(fieldDimension.Key, fieldDimension.Value);

                    _vectorFieldsDimensions = vectorFieldsDimensions;
                    _vectorFieldsDimensionsToWrite = null;
                }
            };
        }
    }
}
