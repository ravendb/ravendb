using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Corax.Querying.Planning;
using Corax.Utils;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Indexes.Persistence.Corax.QueryOptimizer;
using Raven.Server.Documents.Indexes.VectorSearch;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow;
using Sparrow.Json;
using VectorOptions = Raven.Client.Documents.Indexes.Vector.VectorOptions;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    private static void ResolveVectorFromBindings(ClauseExecution exec, ParameterBinding[] slotBindings, BlittableJsonReaderObject queryParameters)
    {
        var bindings = exec.Clause.Bindings;

        var vec = exec.Vector = new VectorParams { Method = exec.Clause.VectorMethod };

        if (bindings.Length > BindingIndex.VectorValue && bindings[BindingIndex.VectorValue] != null)
        {
            var (val, valType) = ResolveBindingRaw(bindings[BindingIndex.VectorValue], slotBindings, queryParameters);
            vec.ResolvedValue = val;
            vec.ResolvedValueType = valType;
            // For scalar parameters, resolve the native type
            if (valType == ParamValueType.Parameter && val is not (BlittableJsonReaderArray or BlittableJsonReaderObject))
            {
                var (resolved, resolvedType) = ResolveParameterValue(val);
                vec.ResolvedValue = resolved;
                vec.ResolvedValueType = ToParamValueType(resolvedType);
            }
        }
        
        if (bindings.Length > BindingIndex.VectorMinMatch && bindings[BindingIndex.VectorMinMatch] != null)
        {
            var (simVal, _) = ResolveBindingScalar(bindings[BindingIndex.VectorMinMatch], slotBindings, queryParameters, builderParameters: null);
            vec.MinimumMatch = simVal switch
            {
                double d => (float)d,
                long l => l,
                _ => -1
            };
        }

        if (bindings.Length > BindingIndex.VectorCandidates && bindings[BindingIndex.VectorCandidates] != null)
        {
            var (candVal, candType) = ResolveBindingScalar(bindings[BindingIndex.VectorCandidates], slotBindings, queryParameters, builderParameters: null);
            if (candType != ParamValueType.Null)
                vec.NumberOfCandidates = Convert.ToInt32(candVal);
        }

        if (bindings.Length > BindingIndex.VectorAiTask && bindings[BindingIndex.VectorAiTask] != null)
        {
            var (taskVal, _) = ResolveBindingScalar(bindings[BindingIndex.VectorAiTask], slotBindings, queryParameters, builderParameters: null);
            vec.AiTaskName = taskVal?.ToString();
        }

    }
    
    private static (object Value, ParamValueType Type) ResolveBindingRaw(ParameterBinding binding, ParameterBinding[] slotBindings, BlittableJsonReaderObject queryParameters)
    {
        binding = slotBindings[binding.ValueOrdinal];
        if (binding.LiteralType != ParamValueType.Parameter)
            return (binding.LiteralValue, binding.LiteralType);
        if (queryParameters.TryGet(binding.ParameterName, out object raw) && raw != null)
            return (raw, ParamValueType.Parameter); // raw from blittable — caller decides how to interpret
        return (null, ParamValueType.Null);
    }

    private static List<CoraxVectorItem> ResolveVectorItems(QueryExecution exec, QueryBuilderParameters builderParams)
    {
        var items = new List<CoraxVectorItem>(exec.VectorSelects.Length);
        foreach (var vec in exec.VectorSelects)
        {
            var item = HandleVector(builderParams, vec);
            item.IsNegated = vec.IsNegated;
            items.Add(item);
        }
        return items;
    }

    private static CoraxVectorItem HandleVector(QueryBuilderParameters builderParameters, ClauseExecution exec)
    {
        Debug.Assert(exec.ClauseType ==ClauseType.Vector);
        IndexField indexField;
        string embeddingsGenerationTaskIdentifier;

        var vec = exec.Vector;
        var minimumMatch = vec.MinimumMatch >= 0
            ? vec.MinimumMatch
            : builderParameters.Index.Configuration.CoraxVectorSearchDefaultMinimumSimilarity;

        int numberOfCandidates = vec.NumberOfCandidates > 0 // NumberOfCandidates == 0 has no meaning
            ? vec.NumberOfCandidates
            : builderParameters.Index.Configuration.CoraxVectorDefaultNumberOfCandidatesForQuerying;

        var fieldName = exec.Clause.FieldName
                        ?? throw new InvalidOperationException("Vector clause has no pre-resolved field name.");

        var fieldMetadata = QueryBuilderHelper.GetFieldMetadata(builderParameters, fieldName, hasBoost: builderParameters.HasBoost);

        // Use pre-resolved vector value and method kind from parsing
        object methodParameter = vec.ResolvedValue;
        ValueTokenType valueTokenType = ToValueTokenType(vec.ResolvedValueType);
        if (vec.Method != VectorSourceKind.Inline)
        {
            var method = vec.Method switch
            {
                VectorSourceKind.FromDocument => VectorHelpers.MethodVectorValue.ForDocument,
                VectorSourceKind.FromText => VectorHelpers.MethodVectorValue.EmbeddingText,
                _ => throw new InvalidDataException($"Unknown vector source kind: {vec.Method}")
            };

            if (method is not VectorHelpers.MethodVectorValue.EmbeddingText)
            {
                return (method, methodParameter) switch
                {
                    (method: VectorHelpers.MethodVectorValue.ForDocument, string docId) => CoraxVectorItem.BuildForDocVector(builderParameters, fieldMetadata, docId, numberOfCandidates, minimumMatch, false),
                    (method: VectorHelpers.MethodVectorValue.ForDocument, StringSegment docIdSegment) => CoraxVectorItem.BuildForDocVector(builderParameters, fieldMetadata, docIdSegment.Value, numberOfCandidates, minimumMatch, false),
                    (method: VectorHelpers.MethodVectorValue.ForRaw, string vectorAsBase64) => CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata,
                        GenerateEmbeddings.FromBase64Array(VectorOptions.Default, builderParameters.Allocator, vectorAsBase64), numberOfCandidates, minimumMatch, false),
                    (method: VectorHelpers.MethodVectorValue.ForRaw, StringSegment stringSegmentAsBase64) => CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata,
                        GenerateEmbeddings.FromBase64Array(VectorOptions.Default, builderParameters.Allocator, stringSegmentAsBase64.ToString()), numberOfCandidates, minimumMatch, false),
                    (_, BlittableJsonReaderArray { Length: 0 }) => throw new InvalidDataException("Cannot perform search on empty value."),
                    _ => throw new InvalidQueryException(
                        $"Unknown method in value ({vec.Method}. Parameter type: {methodParameter?.GetType().FullName}, Value: {methodParameter}")
                };
            }

            embeddingsGenerationTaskIdentifier = vec.AiTaskName;
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
                return CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata, vector.SingleVector.Value, numberOfCandidates, minimumMatch, false);

            return CoraxVectorItem.BuildMultiVector(builderParameters, fieldMetadata, vector.MultiVector, numberOfCandidates, minimumMatch, false);
        }

        // Direct value (not a method call) — use pre-resolved value
        var value = methodParameter;
        var valueType = valueTokenType;

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

            if (builderParameters.Index.IndexFieldsPersistence.TryReadNumberOfDimensions(fieldName, out numberOfDimensions) == false)
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

        if (builderParameters.Index.IndexFieldsPersistence.TryReadNumberOfDimensions(fieldName, out numberOfDimensions) == false)
            return CoraxVectorItem.BuildEmpty(builderParameters); // no vector indexed

        if (transformedEmbeddings.SingleVector != null)
        {
            var singleVector = transformedEmbeddings.SingleVector.Value;

            if (indexField != null)
                AssertDimensions(singleVector);
            return CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata, singleVector, numberOfCandidates, minimumMatch, false);
        }

        if (transformedEmbeddings.MultiVector != null)
        {
            var multiVector = transformedEmbeddings.MultiVector;

            if (indexField != null)
            {
                foreach (var vector in multiVector)
                    AssertDimensions(vector);
            }

            return CoraxVectorItem.BuildMultiVector(builderParameters, fieldMetadata, multiVector, numberOfCandidates, minimumMatch, false);
        }

        throw new InvalidDataException("Expected to get single or multiple embeddings of VectorValue type but none was provided");

        void AssertDimensions(in VectorValue vector)
        {
            // When the query vector's true (pre-packing) dimension count is known, validate it exactly against the
            // field's configured dimensions. This catches sub-byte mismatches on bit-packed Binary fields that the
            // byte-length check below cannot — ceil(dims/8) is not injective, so e.g. 9 and 10 dims both pack to 2 bytes.
            if (vector.SourceDimensions > 0 && indexField.Vector?.Dimensions is int fieldDimensions && vector.SourceDimensions != fieldDimensions)
            {
                using (vector)
                    PortableExceptions.Throw<InvalidDataException>(
                        $"Vector field `{fieldName}` has {fieldDimensions} dimensions, but the vector passed to vector.search() has {vector.SourceDimensions} dimensions.");
            }

            if (numberOfDimensions != vector.Length)
            {
                using (vector)
                    VectorHelpers.ThrowDifferentNumberOfDimensions(indexField, fieldName, vector, numberOfDimensions);
            }
        }
    }
}
