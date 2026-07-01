using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Utils;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Exceptions;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.Indexes.VectorSearch;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow;
using Sparrow.Json;
using VectorOptions = Raven.Client.Documents.Indexes.Vector.VectorOptions;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static class VectorHelpers
{
    public enum MethodVectorValue
    {
        ForDocument,
        ForRaw,
        EmbeddingText
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryRetrieveEmbeddingsGenerationTaskIdentifier(QueryBuilderParameters builderParameters, in string fieldName, out string embeddingsGenerationTaskIdentifier)
    {
        var existsInPersistence =
            builderParameters.Index.IndexFieldsPersistence.TryReadEmbeddingsGenerationTaskIdentifier(fieldName, out embeddingsGenerationTaskIdentifier);

        if (builderParameters.Metadata.IsDynamic == false)
            return existsInPersistence;

        if (((builderParameters.FieldsToFetch != null && builderParameters.FieldsToFetch.IndexFields.TryGetValue(fieldName, out var indexField)) || (builderParameters.Index.Definition.IndexFields.TryGetValue(fieldName, out indexField))) &&
            indexField.Vector is AutoVectorOptions avo)
        {
            embeddingsGenerationTaskIdentifier = avo.EmbeddingsGenerationTaskIdentifier;
            return string.IsNullOrEmpty(avo.EmbeddingsGenerationTaskIdentifier) == false;
        }

        embeddingsGenerationTaskIdentifier = null;
        return false;
    }

    internal static (VectorValue? SingleVector, VectorValue[] MultiVector) GetVectorValueForTextualInput(QueryBuilderParameters parameters, VectorOptions vectorOptions, ValueTokenType valueType, object value)
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

    internal static VectorValue GetVectorValueFromRavenVector(QueryBuilderParameters parameters, BlittableJsonReaderObject json, VectorOptions vectorOptions)
    {
        var vectorObjectFound = json.TryGetMember(Sparrow.Global.Constants.Naming.VectorPropertyName, out var vectorObject);
        PortableExceptions.ThrowIfNot<InvalidDataException>(vectorObjectFound, "Cannot find vector property in the object.");

        var vectorReader = (BlittableJsonReaderVector)vectorObject;
        return QueryBuilderHelper.GetVectorValueFromBlittableJsonVectorReader(parameters.Allocator, vectorOptions, vectorReader);
    }

    internal static VectorValue GetVectorValueFromNumericalBlittableArray(QueryBuilderParameters parameters, BlittableJsonReaderArray array, VectorOptions vectorOptions)
    {
        var bytesUsed = array.Length * (vectorOptions.SourceEmbeddingType is VectorEmbeddingType.Single ? sizeof(float) : 1);
        var memScope = parameters.Allocator.Allocate(bytesUsed, out Memory<byte> mem);

        switch (vectorOptions.SourceEmbeddingType)
        {
            case VectorEmbeddingType.Single:
                CopyFloats(array, MemoryMarshal.Cast<byte, float>(mem.Span));
                break;
            case VectorEmbeddingType.Int8:
                CopyInt8(array, MemoryMarshal.Cast<byte, sbyte>(mem.Span));
                break;
            default:
                CopyBytes(array, mem.Span);
                break;
        }

        var vectorValue = GenerateEmbeddings.FromArray(parameters.Allocator, memScope, mem, vectorOptions, bytesUsed);

        // for Binary, the array length is not exact, so don't set it...
        if (vectorOptions.SourceEmbeddingType is not VectorEmbeddingType.Binary)
            vectorValue.SetSourceDimensions(array.Length);

        return vectorValue;

        static void CopyFloats(BlittableJsonReaderArray src, Span<float> dst)
        {
            ref var dstRef = ref MemoryMarshal.GetReference(dst);
            for (int i = 0; i < src.Length; ++i)
                Unsafe.Add(ref dstRef, i) = src.GetByIndex<float>(i);
        }

        static void CopyInt8(BlittableJsonReaderArray src, Span<sbyte> dst)
        {
            ref var dstRef = ref MemoryMarshal.GetReference(dst);
            for (int i = 0; i < src.Length; ++i)
                Unsafe.Add(ref dstRef, i) = src.GetByIndex<sbyte>(i);
        }

        static void CopyBytes(BlittableJsonReaderArray src, Span<byte> dst)
        {
            ref var dstRef = ref MemoryMarshal.GetReference(dst);
            for (int i = 0; i < src.Length; ++i)
                Unsafe.AddByteOffset(ref dstRef, i) = src.GetByIndex<byte>(i);
        }
    }

    internal static VectorOptions GetExplicitVectorOptions(QueryBuilderParameters builderParameters, in string fieldName, out IndexField indexField)
    {
        if ((builderParameters.FieldsToFetch != null && builderParameters.FieldsToFetch.IndexFields.TryGetValue(fieldName, out indexField)) == false
            && (builderParameters.Index.Definition.IndexFields.TryGetValue(fieldName, out indexField)) == false)
            PortableExceptions.Throw<InvalidDataException>($"Cannot find `{fieldName}` field in the index.");

        return indexField.Vector;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static VectorOptions GetOptions(QueryBuilderParameters builderParameters, in string fieldName, out IndexField indexField)
    {
        if ((builderParameters.FieldsToFetch != null && builderParameters.FieldsToFetch.IndexFields.TryGetValue(fieldName, out indexField)) == false
            && (builderParameters.Index.Definition.IndexFields.TryGetValue(fieldName, out indexField)) == false)
            PortableExceptions.Throw<InvalidDataException>($"Cannot find `{fieldName}` field in the index.");

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
            // Binary is bit-packed: the stored/byte lengths are ceil(dims/8), so report bits to express dimensions.
            VectorEmbeddingType.Binary => (numberOfDimensions * 8, transformedEmbedding.Length * 8),
            _ => throw new InvalidDataException($"Unexpected embedding type - {numberOfDimensions}.")
        };

        PortableExceptions.Throw<InvalidDataException>(
            $"Vector field `{fieldName}` has {storedDimensions} dimensions, but the vector passed to vector.search() has {inputDimensions} dimensions.");
    }

    internal static (VectorValue? SingleVector, VectorValue[] MultiVector) GetEmbeddingsForQueryParameter(QueryBuilderParameters builderParameters, ValueTokenType valueType,
        object value,
        string embeddingsGenerationTaskIdentifier, VectorOptions vectorOptions, string fieldName)
    {
        var database = builderParameters.Index.DocumentDatabase;

        var embeddingsTaskId = new EmbeddingsGenerationTaskIdentifier(embeddingsGenerationTaskIdentifier);

        var embeddingsGenerator = database.EmbeddingsGeneratorQueries;

        var sourceEmbeddingType = embeddingsGenerator.GetQuantizationOf(embeddingsTaskId);

        // Quantized dynamic field indicates that the task generated embeddings with different quantization than requested in the index
        // In this case we want to use quantization defined in dynamic field (which was set in CurrentIndexingScope.GetLoadVectorField)
        VectorEmbeddingType destinationEmbeddingType;
        if (builderParameters.Metadata.IsDynamic)
        {
            destinationEmbeddingType = sourceEmbeddingType is not VectorEmbeddingType.Single ? sourceEmbeddingType : vectorOptions!.DestinationEmbeddingType;
        }
        else
        {
            destinationEmbeddingType = vectorOptions?.DestinationEmbeddingType ?? sourceEmbeddingType;
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
                throw new InvalidQueryException($"Unexpected value provided as parameter to vector.search({fieldName}) method. Got '{value?.GetType().FullName ?? "null"}' type.");
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
