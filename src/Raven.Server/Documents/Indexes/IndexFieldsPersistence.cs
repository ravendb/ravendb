using System;
using System.Collections.Generic;
using System.IO;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Server.ServerWide.Context;
using Sparrow;

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
        
        private Dictionary<string, VectorEmbeddingType> _vectorSourceEmbeddingType;
        private Dictionary<string, VectorEmbeddingType> _vectorSourceEmbeddingTypeToWrite;

        private Dictionary<string, string> _embeddingsGenerationTaskIdentifiers;
        private Dictionary<string, string> _embeddingsGenerationTaskIdentifiersToWrite;
        
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
            _vectorSourceEmbeddingType = _index._indexStorage.ReadIndexEmbeddingType();
            _embeddingsGenerationTaskIdentifiers = _index._indexStorage.ReadVectorSourceAiTaskIdentifiers();
            
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
                        VectorEmbeddingType.Int8 => fieldDimensions.Value + sizeof(float),
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

        internal bool TryReadVectorSourceEmbeddingType(string fieldName, out VectorEmbeddingType embeddingType)
        {
            return _vectorSourceEmbeddingType.TryGetValue(fieldName, out embeddingType);
        }
        
        internal bool TryReadNumberOfDimensions(string fieldName, out int dimensions)
        {
            return _vectorFieldsDimensions.TryGetValue(fieldName, out dimensions);
        }

        internal bool TryReadEmbeddingsGenerationTaskIdentifier(string fieldName, out string taskName)
        {
            return _embeddingsGenerationTaskIdentifiers.TryGetValue(fieldName, out taskName);
        }

        internal void SetEmbeddingsGenerationTaskIdentifier(string fieldName, string taskIdentifier)
        {
            var isStoredOnDisk = _embeddingsGenerationTaskIdentifiers.TryGetValue(fieldName, out var diskStoredSourceEtlTaskName);

            PortableExceptions.ThrowIf<InvalidOperationException>(isStoredOnDisk && diskStoredSourceEtlTaskName != taskIdentifier, $"We are expecting that field {fieldName} has the same ETL task as it was before. However, stored ETL task was {diskStoredSourceEtlTaskName} and current one is {taskIdentifier}.");

            var isStoredInRuntime = false;
            if (_embeddingsGenerationTaskIdentifiersToWrite != null)
            {
                isStoredInRuntime = _embeddingsGenerationTaskIdentifiersToWrite.TryGetValue(fieldName, out var runtimeStoredSourceEtlTaskName);
                PortableExceptions.ThrowIf<InvalidOperationException>(isStoredInRuntime && runtimeStoredSourceEtlTaskName != taskIdentifier,
                    $"We are expecting that field {fieldName} has the same ETL task as it was before. However, previously ETL task was {runtimeStoredSourceEtlTaskName} and current one is {taskIdentifier}.");
            }
            
            if (isStoredOnDisk || isStoredInRuntime)
                return;
            
            _embeddingsGenerationTaskIdentifiersToWrite ??= new Dictionary<string, string>();
            _embeddingsGenerationTaskIdentifiersToWrite.Add(fieldName, taskIdentifier);
        }

        internal void SetVectorSourceEmbeddingType(string fieldName, VectorEmbeddingType embeddingType)
        {
            var isStoredOnDisk = _vectorSourceEmbeddingType.TryGetValue(fieldName, out var diskStoredEmbeddingType);
            
            PortableExceptions.ThrowIf<InvalidOperationException>(isStoredOnDisk && embeddingType != diskStoredEmbeddingType, $"We are expecting that field {fieldName} has the same embedding type as it was before. However, stored embedding type was {diskStoredEmbeddingType} and current one is {embeddingType}.");


            var isStoredInRuntime = false;
            if (_vectorSourceEmbeddingTypeToWrite != null)
            {
                isStoredInRuntime = _vectorSourceEmbeddingTypeToWrite.TryGetValue(fieldName, out var runtimeStoredEmbeddingType);
                PortableExceptions.ThrowIf<InvalidOperationException>(isStoredInRuntime && embeddingType != runtimeStoredEmbeddingType,
                    $"We are expecting that field {fieldName} has the same embedding type as it was before. However, previously embedding type was {runtimeStoredEmbeddingType} and current one is {embeddingType}.");
            }

            if (isStoredOnDisk || isStoredInRuntime)
                return;
            
            _vectorSourceEmbeddingTypeToWrite ??= new Dictionary<string, VectorEmbeddingType>();
            _vectorSourceEmbeddingTypeToWrite.Add(fieldName, embeddingType);
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
                    VectorEmbeddingType.Int8 => (storedDimensions - sizeof(float), dimensions - sizeof(float)),
                    _ => throw new InvalidDataException($"Unexpected embedding type - {destinationEmbeddingType}.")
                };

                throw new InvalidDataException($"Attempted to index embedding with {originalDimensions} dimensions, but field {fieldName} already contains indexed embedding with {originalInputDimensions} dimensions, or was explicitly configured for embeddings with {originalInputDimensions} dimensions.");
            }
            
            _vectorFieldsDimensionsToWrite.Add(fieldName, dimensions);
        }
        
        internal void Persist(TransactionOperationContext indexContext)
        {
            if (_timeFieldsToWrite == null && _vectorFieldsDimensionsToWrite == null && _vectorSourceEmbeddingTypeToWrite == null)
                return;
            
            if (_timeFieldsToWrite != null)
                _index._indexStorage.WriteIndexTimeFields(indexContext.Transaction, _timeFieldsToWrite);
            
            if (_vectorFieldsDimensionsToWrite != null)
                IndexStorage.WriteVectorDimensions(indexContext.Transaction, _vectorFieldsDimensionsToWrite);
            
            if (_vectorSourceEmbeddingTypeToWrite != null)
                IndexStorage.WriteIndexEmbeddingType(indexContext.Transaction, _vectorSourceEmbeddingTypeToWrite);
            
            if (_embeddingsGenerationTaskIdentifiersToWrite != null)
                IndexStorage.WriteEmbeddingsGenerationTaskIdentifiers(indexContext.Transaction, _embeddingsGenerationTaskIdentifiersToWrite);

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

                if (_vectorSourceEmbeddingTypeToWrite != null)
                {
                    var vectorSourceEmbeddingType = new Dictionary<string, VectorEmbeddingType>(_vectorSourceEmbeddingType);
                    foreach (var fieldDimension in _vectorSourceEmbeddingTypeToWrite)
                        vectorSourceEmbeddingType.Add(fieldDimension.Key, fieldDimension.Value);
                    
                    _vectorSourceEmbeddingType = vectorSourceEmbeddingType;
                    _vectorSourceEmbeddingTypeToWrite = null;
                }

                if (_embeddingsGenerationTaskIdentifiersToWrite != null)
                {
                    var vectorSourceEtlTaskName = new Dictionary<string, string>(_embeddingsGenerationTaskIdentifiers);
                    foreach (var fieldAiTaskName in _embeddingsGenerationTaskIdentifiersToWrite)
                        vectorSourceEtlTaskName.Add(fieldAiTaskName.Key, fieldAiTaskName.Value);
                    
                    _embeddingsGenerationTaskIdentifiers = vectorSourceEtlTaskName;
                    _embeddingsGenerationTaskIdentifiersToWrite = null;
                }
            };
        }
    }
}
