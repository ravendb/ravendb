using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Linq;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.Indexes.VectorSearch;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public static partial class CoraxQueryBuilder
{
    private static IQueryMatch HandleVector(Parameters builderParameters, MethodExpression me, bool exact)
    {
        var metadata = builderParameters.Metadata;
        var (value, valueType) = QueryBuilderHelper.GetValue(metadata.Query, metadata, builderParameters.QueryParameters, (ValueExpression)me.Arguments[1],
            allowObjectsInParameters: false, allowArraysInParameters: true);

        var fieldName = metadata.IsDynamic == false
            ? QueryBuilderHelper.ExtractIndexFieldName(metadata.Query, builderParameters.QueryParameters, me.Arguments[0], metadata)
            : metadata.GetVectorFieldName(me, builderParameters.QueryParameters);

        var fieldMetadata = QueryBuilderHelper.GetFieldMetadata(builderParameters, fieldName, hasBoost: builderParameters.HasBoost);
        object transformedEmbeddings = null;
        IndexField indexField = null;

        if (builderParameters.Index.IndexFieldsPersistence.TryReadEmbeddingsGenerationTaskIdentifier(fieldName, out var embeddingsGenerationTaskIdentifier))
        {
            VectorHelpers.ReadEmbeddingFromEmbeddingsGenerationTask(builderParameters, valueType, value, embeddingsGenerationTaskIdentifier, out transformedEmbeddings);
        }
        else
        {
            VectorOptions vectorOptions = VectorHelpers.GetOptions(builderParameters, fieldName, out indexField);
            if (vectorOptions.SourceEmbeddingType is VectorEmbeddingType.Text)
            {
                transformedEmbeddings = VectorHelpers.GetVectorValueForTextualInput(builderParameters, vectorOptions, valueType, value);
            }
            else
            {
                switch (value)
                {
                    case string s:
                        transformedEmbeddings = GenerateEmbeddings.FromBase64Array(vectorOptions, builderParameters.Allocator, s);
                        break;
                    case StringSegment stringSegment:
                        transformedEmbeddings = GenerateEmbeddings.FromBase64Array(vectorOptions, builderParameters.Allocator, stringSegment.ToString());
                        break;
                    case BlittableJsonReaderObject bjro:
                        transformedEmbeddings = VectorHelpers.GetVectorValueFromRavenVector(builderParameters, bjro, vectorOptions);
                        break;
                    case BlittableJsonReaderArray { Length: > 0 } bjra:
                    {
                        var isRavenVector = bjra[0] is BlittableJsonReaderObject;
                        var isStringArray = bjra[0] is string or StringSegment or LazyStringValue;
                        var isArray = bjra[0] is BlittableJsonReaderArray;

                        if (isRavenVector == false && isStringArray == false && isArray == false)
                        {
                            transformedEmbeddings = VectorHelpers.GetVectorValueFromNumericalBlittableArray(builderParameters, bjra, vectorOptions);
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

                            transformedEmbeddings = embeddings;
                        }

                        break;
                    }
                    default:
                        PortableExceptions.Throw<InvalidDataException>("We expected to get vector(s), however got: " + value.GetType().Name);
                        break;
                }
            }
        }

        var minimumMatch = builderParameters.Index.Configuration.CoraxVectorSearchDefaultMinimumSimilarity;
        if (me.Arguments.Count > 2)
        {
            (value, valueType) = QueryBuilderHelper.GetValue(builderParameters.Metadata.Query, builderParameters.Metadata, builderParameters.QueryParameters,
                (ValueExpression)me.Arguments[2]);
            minimumMatch = valueType switch
            {
                ValueTokenType.Null => builderParameters.Index.Configuration.CoraxVectorSearchDefaultMinimumSimilarity,
                ValueTokenType.Long => (long)value,
                ValueTokenType.Double => (float)(double)value,
                _ => throw new NotSupportedException("vector.search() minimumMatch must be a float, but was: " + valueType)
            };
        }

        int numberOfCandidates = builderParameters.Index.Configuration.CoraxVectorDefaultNumberOfCandidatesForQuerying;
        if (me.Arguments.Count > 3)
        {
            (value, valueType) = QueryBuilderHelper.GetValue(builderParameters.Metadata.Query, builderParameters.Metadata, builderParameters.QueryParameters,
                (ValueExpression)me.Arguments[3]);
            numberOfCandidates = valueType switch
            {
                ValueTokenType.Long => Convert.ToInt32(value),
                ValueTokenType.Double => Convert.ToInt32(value),
                ValueTokenType.Null => builderParameters.Index.Configuration.CoraxVectorDefaultNumberOfCandidatesForQuerying,
                _ => throw new NotSupportedException("vector.search() minimumMatch must be a float, but was: " + valueType)
            };
        }

        if (builderParameters.Index.IndexFieldsPersistence.TryReadNumberOfDimensions(fieldName, out var numberOfDimensions) == false)
            return builderParameters.IndexSearcher.EmptyMatch(); // no vector indexed

        switch (transformedEmbeddings)
        {
            case VectorValue singleVector:
                if (indexField != null)
                    AssertDimensions(singleVector);
                
                return builderParameters.IndexSearcher.VectorSearch(fieldMetadata, singleVector, minimumMatch, numberOfCandidates, exact,
                    builderParameters.IsVectorSingleClause);
            case VectorValue[] multiVector:
            {
                if (indexField != null)
                {
                    foreach (var vector in multiVector)
                        AssertDimensions(vector);
                }

                return builderParameters.IndexSearcher.MultiVectorSearch(fieldMetadata, multiVector, minimumMatch, numberOfCandidates, exact,
                    builderParameters.IsVectorSingleClause);
            }
        }

        throw new InvalidDataException("Expected a VectorValue(s), but got: " + transformedEmbeddings.GetType().Name);

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
        internal static object GetVectorValueForTextualInput(Parameters parameters, VectorOptions vectorOptions, ValueTokenType valueType, object value)
        {
            object result = null;
            if (valueType is ValueTokenType.String)
                result = GenerateEmbeddings.FromText(parameters.Allocator, vectorOptions, value.ToString());
            else
            {
                if (valueType is not ValueTokenType.Parameter)
                    PortableExceptions.Throw<InvalidDataException>($"Cannot use vector.search() on a text field with a non-string value. Got {valueType}");

                if (value is BlittableJsonReaderArray valueAsList)
                {
                    var embeddings = new VectorValue[valueAsList.Length];
                    for (var i = 0; i < valueAsList.Length; ++i)
                        embeddings[i] = GenerateEmbeddings.FromText(parameters.Allocator, vectorOptions, valueAsList[i].ToString());
                    result = embeddings;
                }
                else
                    PortableExceptions.Throw<InvalidDataException>($"Cannot use vector.search() on a text field with a non-string value(s). Got {valueType}");
            }

            return result;
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
            return vectorSourceEmbeddingType switch
            {
                VectorEmbeddingType.Single => VectorOptions.Default,
                VectorEmbeddingType.Text => VectorOptions.DefaultText,
                _ => throw new InvalidDataException(
                    $"Unknown vector source embedding type: {vectorSourceEmbeddingType}. Implicit configuration support only single and text vector source embedding types.")
            };
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

        internal static void ReadEmbeddingFromEmbeddingsGenerationTask(Parameters builderParameters, ValueTokenType valueType, object value,
            string embeddingsGenerationTaskIdentifier, VectorOptions vectorOptions,
            out object transformedEmbedding)
        {
            var database = builderParameters.Index.DocumentDatabase;
            
            var valueAsString = valueType switch
            {
                ValueTokenType.String => value.ToString(),
                _ => throw new NotSupportedException("Vector.Search() on " + valueType)
            };

            var embeddingsTaskId = new EmbeddingsGenerationTaskIdentifier(embeddingsGenerationTaskIdentifier);
            var connectionStringId = database.AiIntegrations.GetConnectionStringByEmbeddingsGenerationTask(embeddingsTaskId); // TODO michal
            if (builderParameters.Index.DocumentDatabase.AiIntegrations.TryGetEmbeddingsGenerationConfiguration(embeddingsTaskId, out var embeddingsGenerationConfiguration) == false)
                PortableExceptions.Throw<InvalidDataException>($"Cannot find embeddings generation configuration for {embeddingsTaskId.Value}.");            
            
            transformedEmbedding = database.AiIntegrations.Embeddings
                .GetEmbeddingsForQueryAsync(builderParameters.DocumentsContext, connectionStringId, embeddingsTaskId, valueAsString, vectorOptions)
                .GetAwaiter().GetResult();
        }

        private static bool TryGetEmbeddingFromCache(Parameters builderParameters, DocumentsOperationContext documentContext, ByteStringContext embeddingContext,
            string valueAsString,
            AiConnectionStringIdentifier aiConnectionStringIdentifier, EmbeddingsGenerationTaskIdentifier embeddingsGenerationTaskIdentifier, out object transformedEmbedding)
        {
            transformedEmbedding = null;
            if (builderParameters.Index.DocumentDatabase.AiIntegrations.TryGetEmbeddingsGenerationConfiguration(embeddingsGenerationTaskIdentifier, out var configuration) == false)
                PortableExceptions.Throw<InvalidDataException>($"Cannot find embeddings generation configuration for {embeddingsGenerationTaskIdentifier.Value}.");
            var hash = EmbeddingsHelper.CalculateInputValueHash(valueAsString);
            var id = EmbeddingsHelper.GetEmbeddingCacheDocumentId(aiConnectionStringIdentifier, hash, configuration.TargetQuantizationType);

            using (documentContext.OpenReadTransaction())
            {
                var valueEmbeddingsDocument = builderParameters.DocumentsContext.DocumentDatabase.DocumentsStorage.Get(documentContext, id);

                if (valueEmbeddingsDocument != null)
                {
                    if (valueEmbeddingsDocument.Data.TryGet(valueAsString, out string attachmentName))
                    {
                        var attachment = builderParameters.DocumentsContext.DocumentDatabase.DocumentsStorage.AttachmentsStorage.GetAttachment(documentContext, id,
                            attachmentName, AttachmentType.Document, null);

                        var configuration = builderParameters.Index.DocumentDatabase.AiIntegrations.GetEmbeddingsGenerationConfiguration(embeddingsGenerationTaskIdentifier);

                        var bytesRequired = (int)attachment.Size;
                        var memScope = embeddingContext.Allocate(bytesRequired, out Memory<byte> memory);
                        attachment.Stream.ReadExactly(memory.Span);
                        transformedEmbedding = GenerateEmbeddings.Quantize(embeddingContext, configuration.TargetQuantizationType, memScope, memory, bytesRequired);

                        return true;
                    }
                }
            }

            return false;
        }
    }
}
