using System;
using System.IO;
using System.Text;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Queries;
using Sparrow.Extensions;

namespace Raven.Client.Documents.Session.Tokens;

public sealed class VectorSearchToken : WhereToken
{
    private float SimilarityThreshold { get; set; }
    private EmbeddingType SourceQuantizationType { get; set; }
    private EmbeddingType TargetQuantizationType { get; set; }
    private EmbeddingType QueriedVectorQuantizationType { get; set; }
    private bool IsSourceBase64Encoded { get; set; }
    private bool IsVectorBase64Encoded { get; set; }
    
    public VectorSearchToken(string fieldName, string parameterName, EmbeddingType sourceQuantizationType, EmbeddingType targetQuantizationType, EmbeddingType queriedQueriedVectorQuantizationType, bool isSourceBase64Encoded, bool isVectorBase64Encoded, float similarityThreshold)
    {
        FieldName = fieldName;
        ParameterName = parameterName;
        
        SourceQuantizationType = sourceQuantizationType;
        TargetQuantizationType = targetQuantizationType;
        QueriedVectorQuantizationType = queriedQueriedVectorQuantizationType;
                
        IsSourceBase64Encoded = isSourceBase64Encoded;
        IsVectorBase64Encoded = isVectorBase64Encoded;
                
        SimilarityThreshold = similarityThreshold;
    }
    
    public override void WriteTo(StringBuilder writer)
    {
        writer.Append("vector.search(");
        if (SourceQuantizationType is EmbeddingType.Float32 && TargetQuantizationType is EmbeddingType.Float32)
            writer.Append(FieldName);
        else
        {
            var methodName = (SourceQuantizationType, TargetQuantizationType) switch
            {
                (EmbeddingType.Float32, EmbeddingType.Int8) => Constants.VectorSearch.EmbeddingSingleInt8,
                (EmbeddingType.Float32, EmbeddingType.Binary) => Constants.VectorSearch.EmbeddingSingleInt1,
                (EmbeddingType.Text, EmbeddingType.Float32) => Constants.VectorSearch.EmbeddingText,
                (EmbeddingType.Text, EmbeddingType.Int8) => Constants.VectorSearch.EmbeddingTextInt8,
                (EmbeddingType.Text, EmbeddingType.Binary) => Constants.VectorSearch.EmbeddingTextInt1,
                (EmbeddingType.Int8, EmbeddingType.Int8) => Constants.VectorSearch.EmbeddingInt8,
                (EmbeddingType.Binary, EmbeddingType.Binary) => Constants.VectorSearch.EmbeddingInt8,
                _ => throw new InvalidOperationException(
                    $"Cannot create vector field with SourceQuantizationType {SourceQuantizationType} and TargetQuantizationType {TargetQuantizationType}")
            };
            
            writer.Append($"{methodName}({FieldName})");
        }
        
        
        writer.Append($", ${ParameterName}");

        if (SimilarityThreshold.AlmostEquals(Constants.VectorSearch.MinimumSimilarity) == false)
            writer.Append($", {SimilarityThreshold}");
        
        writer.Append(')');
    }
}
