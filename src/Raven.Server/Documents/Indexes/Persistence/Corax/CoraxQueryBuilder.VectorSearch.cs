using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Raven.Client;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Exceptions;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.Indexes.Persistence.Corax.QueryOptimizer;
using Raven.Server.Documents.Indexes.VectorSearch;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public static partial class CoraxQueryBuilder
{
    private static CoraxVectorItem HandleVector(Parameters builderParameters, MethodExpression me, bool exact)
    {
        var metadata = builderParameters.Metadata;
        IndexField indexField;
        string embeddingsGenerationTaskIdentifier;
        var minimumMatch = builderParameters.Index.Configuration.CoraxVectorSearchDefaultMinimumSimilarity;
        if (me.Arguments.Count > 2)
        {
            var (similarityValue, similiarityValueType) = QueryBuilderHelper.GetValue(builderParameters.Metadata.Query, builderParameters.Metadata, builderParameters.QueryParameters,
                (ValueExpression)me.Arguments[2]);
            minimumMatch = similiarityValueType switch
            {
                ValueTokenType.Null => builderParameters.Index.Configuration.CoraxVectorSearchDefaultMinimumSimilarity,
                ValueTokenType.Long => (long)similarityValue,
                ValueTokenType.Double => (float)(double)similarityValue,
                _ => throw new NotSupportedException("vector.search() minimumMatch must be a float, but was: " + similiarityValueType)
            };
        }

        int numberOfCandidates = builderParameters.Index.Configuration.CoraxVectorDefaultNumberOfCandidatesForQuerying;
        if (me.Arguments.Count > 3)
        {
            var (candidatesValue, candidatesValueType) = QueryBuilderHelper.GetValue(builderParameters.Metadata.Query, builderParameters.Metadata, builderParameters.QueryParameters,
                (ValueExpression)me.Arguments[3]);
            numberOfCandidates = candidatesValueType switch
            {
                ValueTokenType.Long => Convert.ToInt32(candidatesValue),
                ValueTokenType.Double => Convert.ToInt32(candidatesValue),
                ValueTokenType.Null => builderParameters.Index.Configuration.CoraxVectorDefaultNumberOfCandidatesForQuerying,
                _ => throw new NotSupportedException("vector.search() minimumMatch must be a float, but was: " + candidatesValueType)
            };
        }
        
        var fieldName = metadata.IsDynamic == false
            ? QueryBuilderHelper.ExtractIndexFieldName(metadata.Query, builderParameters.QueryParameters, me.Arguments[0], metadata)
            : metadata.GetVectorFieldName(me, builderParameters.QueryParameters);

        var fieldMetadata = QueryBuilderHelper.GetFieldMetadata(builderParameters, fieldName, hasBoost: builderParameters.HasBoost);
        QueryExpression srcVector = me.Arguments[1];

        if (srcVector is MethodExpression methodValue) // embedding.forDoc(docId) ...
        {
            var supportedMethods = methodValue.Name != Constants.VectorSearch.EmbeddingForDocument
                                   && methodValue.Name != Constants.VectorSearch.EmbeddingForRaw
                                   && methodValue.Name != Constants.VectorSearch.EmbeddingText;
                
            PortableExceptions.ThrowIf<InvalidDataException>(supportedMethods,
                $"Expected {Constants.VectorSearch.EmbeddingForDocument}() method call, but got: {methodValue.Name}");

            var (methodParameter, valueTokenType) = QueryBuilderHelper.GetValue(metadata.Query, metadata, builderParameters.QueryParameters, (ValueExpression)methodValue.Arguments[0], allowObjectsInParameters: false, allowArraysInParameters: true);
            
            var method = methodValue.Name.ToString() switch
            {
                Constants.VectorSearch.EmbeddingForDocument => VectorHelpers.MethodVectorValue.ForDocument,
                Constants.VectorSearch.EmbeddingForRaw => VectorHelpers.MethodVectorValue.ForRaw,
                Constants.VectorSearch.EmbeddingText => VectorHelpers.MethodVectorValue.EmbeddingText,
                _ => throw new InvalidDataException(
                    $"Unknown method in value ({methodValue.Name}. Parameter type: {methodParameter.GetType().FullName}, Value: {methodParameter}")
            };

            if (method is not VectorHelpers.MethodVectorValue.EmbeddingText)
            {
                return (method, methodParameter) switch
                {
                    (method: VectorHelpers.MethodVectorValue.ForDocument, string docId) => CoraxVectorItem.BuildForDocVector(builderParameters, fieldMetadata, docId, numberOfCandidates, minimumMatch, exact),
                    (method: VectorHelpers.MethodVectorValue.ForDocument, StringSegment docIdSegment) => CoraxVectorItem.BuildForDocVector(builderParameters, fieldMetadata, docIdSegment.Value, numberOfCandidates, minimumMatch, exact),
                    (method: VectorHelpers.MethodVectorValue.ForRaw, string vectorAsBase64) => CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata, GenerateEmbeddings.FromBase64Array(VectorOptions.Default, builderParameters.Allocator, vectorAsBase64, false), numberOfCandidates, minimumMatch, exact),
                    (method: VectorHelpers.MethodVectorValue.ForRaw, StringSegment stringSegmentAsBase64) => CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata, GenerateEmbeddings.FromBase64Array(VectorOptions.Default, builderParameters.Allocator, stringSegmentAsBase64.ToString(), false), numberOfCandidates, minimumMatch, exact),
                    (_, BlittableJsonReaderArray { Length: > 0 }) => throw new InvalidDataException("Cannot perform search on empty value."),
                    _ => throw new InvalidQueryException(
                        $"Unknown method in value ({methodValue.Name}. Parameter type: {methodParameter.GetType().FullName}, Value: {methodParameter}")
                };
            }

            var aiTaskMethod = (MethodExpression)methodValue.Arguments[1];
            PortableExceptions.ThrowIfNot<InvalidOperationException>(aiTaskMethod.Name == Constants.VectorSearch.AiTaskMethodName, "Expected to find an AI task method call, but got: " + aiTaskMethod.Name);
            embeddingsGenerationTaskIdentifier = QueryBuilderHelper.GetValue(metadata.Query, metadata, builderParameters.QueryParameters, (ValueExpression)aiTaskMethod.Arguments[0],
                allowObjectsInParameters: false, allowArraysInParameters: false).Value.ToString();
            var vectorOptions = VectorHelpers.GetExplicitVectorOptions(builderParameters, fieldName, out indexField);
            if (vectorOptions != null)
            {
                vectorOptions = new VectorOptions()
                {
                    DestinationEmbeddingType = vectorOptions.DestinationEmbeddingType,
                    Dimensions = vectorOptions.Dimensions,
                    SourceEmbeddingType = VectorEmbeddingType.Text,
                    NumberOfCandidatesForIndexing = vectorOptions.NumberOfCandidatesForIndexing,
                    NumberOfEdges = vectorOptions.NumberOfEdges
                };
            }
            
            var vector = VectorHelpers.GetEmbeddingsForQueryParameter(builderParameters, valueTokenType, methodParameter, embeddingsGenerationTaskIdentifier, vectorOptions, fieldName);

            if (vector.SingleVector != null)
            {
                return CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata, vector.SingleVector.Value, numberOfCandidates, minimumMatch, exact);
            }

            return CoraxVectorItem.BuildMultiVector(builderParameters, fieldMetadata, vector.MultiVector, numberOfCandidates, minimumMatch, exact);
            // We need to handle embedding generation from embedding.text()
        }
        
        var (value, valueType) = QueryBuilderHelper.GetValue(metadata.Query, metadata, builderParameters.QueryParameters, (ValueExpression)srcVector,
            allowObjectsInParameters: false, allowArraysInParameters: true);

        (VectorValue? SingleVector, VectorValue[] MultiVector) transformedEmbeddings = (null, null);
        int numberOfDimensions;
        if (VectorHelpers.TryRetrieveEmbeddingsGenerationTaskIdentifier(builderParameters, fieldName, out embeddingsGenerationTaskIdentifier))
        {
            var vectorOptions = VectorHelpers.GetExplicitVectorOptions(builderParameters, fieldName, out indexField);
            transformedEmbeddings = VectorHelpers.GetEmbeddingsForQueryParameter(builderParameters, valueType, value, embeddingsGenerationTaskIdentifier, vectorOptions, fieldName);
        }
        else
        {
            VectorOptions vectorOptions = VectorHelpers.GetOptions(builderParameters, fieldName, out indexField);
            
            if (builderParameters.Index.IndexFieldsPersistence.TryReadNumberOfDimensions(fieldName, out  numberOfDimensions) == false)
                return CoraxVectorItem.BuildEmpty(builderParameters); // no vector indexed
            if (vectorOptions.SourceEmbeddingType is VectorEmbeddingType.Text)
            {
                transformedEmbeddings = VectorHelpers.GetVectorValueForTextualInput(builderParameters, vectorOptions, valueType, value);
            }
            else
            {
                switch (value)
                {
                    case string s:
                        transformedEmbeddings.SingleVector = GenerateEmbeddings.FromBase64Array(vectorOptions, builderParameters.Allocator, s);
                        break;
                    case StringSegment stringSegment:
                        transformedEmbeddings.SingleVector = GenerateEmbeddings.FromBase64Array(vectorOptions, builderParameters.Allocator, stringSegment.ToString());
                        break;
                    case BlittableJsonReaderObject bjro:
                        transformedEmbeddings.SingleVector = VectorHelpers.GetVectorValueFromRavenVector(builderParameters, bjro, vectorOptions);
                        break;
                    case BlittableJsonReaderArray { Length: > 0 } bjra:
                    {
                        var isRavenVector = bjra[0] is BlittableJsonReaderObject;
                        var isStringArray = bjra[0] is string or StringSegment or LazyStringValue;
                        var isArray = bjra[0] is BlittableJsonReaderArray;

                        if (isRavenVector == false && isStringArray == false && isArray == false)
                        {
                            transformedEmbeddings.SingleVector = VectorHelpers.GetVectorValueFromNumericalBlittableArray(builderParameters, bjra, vectorOptions);
                        }
                        else
                        {
                            var embeddings = new VectorValue[bjra.Length];
                            for (int i = 0; i < bjra.Length; ++i)
                            {
                                if (isRavenVector)
                                    embeddings[i] = VectorHelpers.GetVectorValueFromRavenVector(builderParameters, (BlittableJsonReaderObject)bjra[i], vectorOptions);
                                else if (isStringArray)
                                    embeddings[i] = GenerateEmbeddings.FromBase64Array(vectorOptions, builderParameters.Allocator, bjra[i].ToString());
                                else
                                    embeddings[i] = VectorHelpers.GetVectorValueFromNumericalBlittableArray(builderParameters, (BlittableJsonReaderArray)bjra[i],
                                        vectorOptions);
                            }

                            transformedEmbeddings.MultiVector = embeddings;
                        }

                        break;
                    }
                    default:
                        PortableExceptions.Throw<InvalidDataException>("We expected to get vector(s), however got: " + value.GetType().Name);
                        break;
                }
            }
        }

        if (builderParameters.Index.IndexFieldsPersistence.TryReadNumberOfDimensions(fieldName, out  numberOfDimensions) == false)
            return CoraxVectorItem.BuildEmpty(builderParameters); // no vector indexed

        if (transformedEmbeddings.SingleVector != null)
        {
            var singleVector = transformedEmbeddings.SingleVector.Value;

            if (indexField != null)
                AssertDimensions(singleVector);
            return CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata, singleVector, numberOfCandidates, minimumMatch, exact);
        }

        if (transformedEmbeddings.MultiVector != null)
        {
            var multiVector = transformedEmbeddings.MultiVector;

            if (indexField != null)
            {
                foreach (var vector in multiVector)
                    AssertDimensions(vector);
            }
            return CoraxVectorItem.BuildMultiVector(builderParameters, fieldMetadata, multiVector, numberOfCandidates, minimumMatch, exact);
        }

        throw new InvalidDataException("Expected to get single or multiple embeddings of VectorValue type but none was provided");

        void AssertDimensions(in VectorValue vector)
        {
            if (numberOfDimensions != vector.Length)
            {
                using (vector)
                    VectorHelpers.ThrowDifferentNumberOfDimensions(indexField, fieldName, vector, numberOfDimensions);
            }
        }
    }

    private static class VectorHelpers
    {
        public enum MethodVectorValue
        {
            ForDocument,
            ForRaw,
            EmbeddingText
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryRetrieveEmbeddingsGenerationTaskIdentifier(Parameters builderParameters, in string fieldName, out string embeddingsGenerationTaskIdentifier)
        {
            var existsInPersistence =
                builderParameters.Index.IndexFieldsPersistence.TryReadEmbeddingsGenerationTaskIdentifier(fieldName, out embeddingsGenerationTaskIdentifier);

            if (builderParameters.Metadata.IsDynamic == false)
                return existsInPersistence;

            if (((builderParameters.FieldsToFetch != null && builderParameters.FieldsToFetch.IndexFields.TryGetValue(fieldName, out var indexField)) || builderParameters.Index.Definition.IndexFields.TryGetValue(fieldName, out indexField)) && indexField.Vector is AutoVectorOptions avo)
            {
                embeddingsGenerationTaskIdentifier = avo.EmbeddingsGenerationTaskIdentifier;
                return avo.EmbeddingsGenerationTaskIdentifier != null;            
            }
            
            embeddingsGenerationTaskIdentifier = null;
            return false;
        }
        
        internal static (VectorValue? SingleVector, VectorValue[] MultiVector) GetVectorValueForTextualInput(Parameters parameters, VectorOptions vectorOptions, ValueTokenType valueType, object value)
        {
            if (valueType is ValueTokenType.String)
                return (GenerateEmbeddings.FromText(parameters.Allocator, vectorOptions, value.ToString()), null);
            
            if (valueType is not ValueTokenType.Parameter)
                PortableExceptions.Throw<InvalidDataException>($"Cannot use vector.search() on a text field with a non-string value. Got {valueType}");

            if (value is BlittableJsonReaderArray valueAsList)
            {
                var embeddings = new VectorValue[valueAsList.Length];
                for (var i = 0; i < valueAsList.Length; ++i)
                    embeddings[i] = GenerateEmbeddings.FromText(parameters.Allocator, vectorOptions, valueAsList[i].ToString());

                return (null, embeddings);
            }

            PortableExceptions.Throw<InvalidDataException>($"Cannot use vector.search() on a text field with a non-string value(s). Got {valueType}");
            return (null, null);
        }

        internal static VectorValue GetVectorValueFromRavenVector(Parameters parameters, BlittableJsonReaderObject json, VectorOptions vectorOptions)
        {
            var vectorObjectFound = json.TryGetMember(Sparrow.Global.Constants.Naming.VectorPropertyName, out var vectorObject);
            PortableExceptions.ThrowIfNot<InvalidDataException>(vectorObjectFound, "Cannot find vector property in the object.");

            var vectorReader = (BlittableJsonReaderVector)vectorObject;
            return QueryBuilderHelper.GetVectorValueFromBlittableJsonVectorReader(parameters.Allocator, vectorOptions, vectorReader);
        }

        internal static VectorValue GetVectorValueFromNumericalBlittableArray(Parameters parameters, BlittableJsonReaderArray array, VectorOptions vectorOptions)
        {
            var bytesUsed = array.Length * (vectorOptions.SourceEmbeddingType is VectorEmbeddingType.Single ? sizeof(float) : 1);
            var memScope = parameters.Allocator.Allocate(bytesUsed, out Memory<byte> mem);
            ref var floatRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, float>(mem.Span));
            ref var sbyteRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, sbyte>(mem.Span));
            ref var byteRef = ref MemoryMarshal.GetReference(mem.Span);

            for (int i = 0; i < array.Length; ++i)
            {
                switch (vectorOptions.SourceEmbeddingType)
                {
                    case VectorEmbeddingType.Single:
                        Unsafe.Add(ref floatRef, i) = array.GetByIndex<float>(i);
                        break;
                    case VectorEmbeddingType.Int8:
                        Unsafe.Add(ref sbyteRef, i) = array.GetByIndex<sbyte>(i);
                        break;
                    default:
                        Unsafe.AddByteOffset(ref byteRef, i) = array.GetByIndex<byte>(i);
                        break;
                }
            }

            return GenerateEmbeddings.FromArray(parameters.Allocator, memScope, mem, vectorOptions, bytesUsed);
        }

        internal static VectorOptions GetExplicitVectorOptions(Parameters builderParameters, in string fieldName, out IndexField indexField)
        {
            if ((builderParameters.FieldsToFetch != null && builderParameters.FieldsToFetch.IndexFields.TryGetValue(fieldName, out indexField)) == false
                && (builderParameters.Index.Definition.IndexFields.TryGetValue(fieldName, out indexField)) == false)
                PortableExceptions.Throw<InvalidDataException>($"Cannot find `{fieldName}` field in the index.");
            
            return indexField.Vector;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static VectorOptions GetOptions(Parameters builderParameters, in string fieldName, out IndexField indexField)
        {
            if ((builderParameters.FieldsToFetch != null && builderParameters.FieldsToFetch.IndexFields.TryGetValue(fieldName, out indexField)) == false
                && (builderParameters.Index.Definition.IndexFields.TryGetValue(fieldName, out indexField)) == false)
                PortableExceptions.Throw<InvalidDataException>($"Cannot find `{fieldName}` field in the index.");

            // VectorOptions can be null when a user does not specify the configuration.
            // In such cases, we will choose the input depending on the value type (similar to how we handle it during indexing).
            if (indexField.Vector != null)
                return indexField.Vector;

            builderParameters.Index.IndexFieldsPersistence.TryReadVectorSourceEmbeddingType(fieldName, out var vectorSourceEmbeddingType);
            
            var defaultVectorOptions = vectorSourceEmbeddingType switch
            {
                VectorEmbeddingType.Single => VectorOptions.Default,
                VectorEmbeddingType.Text => VectorOptions.DefaultText,
                _ => throw new InvalidDataException(
                    $"Unknown vector source embedding type: {vectorSourceEmbeddingType}. Implicit configuration support only single and text vector source embedding types.")
            };

            indexField.Vector = defaultVectorOptions;
            
            return defaultVectorOptions;
        }

        internal static void ThrowDifferentNumberOfDimensions(in IndexField indexField, in string fieldName, in VectorValue transformedEmbedding,
            in int numberOfDimensions)
        {
            var (storedDimensions, inputDimensions) = indexField.Vector.DestinationEmbeddingType switch
            {
                VectorEmbeddingType.Single => (numberOfDimensions / sizeof(float), transformedEmbedding.Length / sizeof(float)),
                VectorEmbeddingType.Int8 => (numberOfDimensions - sizeof(float), transformedEmbedding.Length - sizeof(float)),
                VectorEmbeddingType.Binary => (numberOfDimensions, transformedEmbedding.Length),
                _ => throw new InvalidDataException($"Unexpected embedding type - {numberOfDimensions}.")
            };

            PortableExceptions.Throw<InvalidDataException>(
                $"Vector field `{fieldName}` has {storedDimensions} dimensions, but the vector passed to vector.search() has {inputDimensions} dimensions.");
        }

        internal static (VectorValue? SingleVector, VectorValue[] MultiVector) GetEmbeddingsForQueryParameter(Parameters builderParameters, ValueTokenType valueType,
            object value,
            string embeddingsGenerationTaskIdentifier, VectorOptions vectorOptions, string fieldName)
        {
            var database = builderParameters.Index.DocumentDatabase;
            
            var embeddingsTaskId = new EmbeddingsGenerationTaskIdentifier(embeddingsGenerationTaskIdentifier);
            
            var embeddingsGenerator = database.EmbeddingsGeneratorQueries;

            if (embeddingsGenerator.EmbeddingTaskExists(embeddingsTaskId) == false)
            {
                var taskConfiguration = database.EtlLoader.EmbeddingsGenerationDestinations
                    .SingleOrDefault(x => x.Identifier == embeddingsGenerationTaskIdentifier);

                if (taskConfiguration is null)
                    throw new InvalidQueryException(
                        $"Couldn't find Embeddings Generation task with '{embeddingsGenerationTaskIdentifier}' identifier");

                if (taskConfiguration.Disabled)
                    throw new InvalidQueryException(
                        $"Embeddings Generation task with '{embeddingsGenerationTaskIdentifier}' identifier is disabled, and cannot be used for querying");
            }

            var sourceEmbeddingType = embeddingsGenerator.GetQuantizationOf(embeddingsTaskId);

            // Quantized dynamic field indicates that the task generated embeddings with different quantization than requested in the index
            // In this case we want to use quantization defined in dynamic field (which was set in CurrentIndexingScope.GetLoadVectorField)
            VectorEmbeddingType destinationEmbeddingType;
            if (builderParameters.Metadata.IsDynamic)
            {
                if (sourceEmbeddingType is not VectorEmbeddingType.Single)
                    destinationEmbeddingType = sourceEmbeddingType;
                else
                    destinationEmbeddingType = vectorOptions!.DestinationEmbeddingType;
            }
            else
            {
                if (vectorOptions?.DestinationEmbeddingType is not null)
                    destinationEmbeddingType = vectorOptions!.DestinationEmbeddingType;
                else
                    destinationEmbeddingType = sourceEmbeddingType;
            }
            
            ReadOnlyMemory<ReadOnlyMemory<byte>> embeddingValues;

            switch (valueType)
            {
                case ValueTokenType.String:
                    embeddingValues = embeddingsGenerator
                        .GetEmbeddingsForQuery(builderParameters.DocumentsContext, embeddingsTaskId, value.ToString());
                    break;
                case ValueTokenType.Parameter:
                {
                    if (value is not BlittableJsonReaderArray bjra)
                        throw new InvalidQueryException($"Expected array as parameter of vector.search({fieldName}) method, got '{value.GetType().FullName}' type instead.");
                
                    var values = new string[bjra.Length];

                    for (var i = 0; i < values.Length; i++)
                        values[i] = bjra[i].ToString();
                
                    embeddingValues = embeddingsGenerator
                        .GetEmbeddingsForQuery(builderParameters.DocumentsContext, embeddingsTaskId, values);
                    break;
                }
                default:
                    throw new NotSupportedException($"Unexpected value type provided as parameter to vector.search({fieldName}) method. Got '{value.GetType().FullName}' type.");
            }
            
            var queryingVectorOption = new VectorOptions
            {
                SourceEmbeddingType = sourceEmbeddingType,
                DestinationEmbeddingType = destinationEmbeddingType
            };

            if (embeddingValues.Length == 1)
            {
                var embeddingValue = embeddingValues.Span[0];

                return (GenerateEmbeddings.FromArray(builderParameters.Allocator, embeddingValue.Span, queryingVectorOption), null);
            }
            else
            {
                var vectorValues = new VectorValue[embeddingValues.Length];

                for (int i = 0; i < embeddingValues.Length; i++)
                {
                    var embeddingValue = embeddingValues.Span[i];

                    vectorValues[i] = GenerateEmbeddings.FromArray(builderParameters.Allocator, embeddingValue.Span, queryingVectorOption);
                }

                return (null, vectorValues);
            }
        }
    }
}
