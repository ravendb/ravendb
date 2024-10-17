using System.Text;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Queries;

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
        bool explicitSourceQuantizationType = false;
        
        writer.Append("vector.search(");

        if (IsSourceBase64Encoded)
            writer.Append("base64(");
        
        if (SourceQuantizationType == EmbeddingType.None)
        {
            if (TargetQuantizationType != EmbeddingType.None)
            {
                // TODO
                writer.Append($"f32_{TargetQuantizationType.ToString().ToLower()}(");

                explicitSourceQuantizationType = true;
            }
        }
        
        else if (TargetQuantizationType != EmbeddingType.None)
        {
            writer.Append($"{SourceQuantizationType.ToString().ToLower()}(");
            
            explicitSourceQuantizationType = true;
        }
        
        writer.Append(FieldName);

        if (IsSourceBase64Encoded)
            writer.Append(')');

        if (explicitSourceQuantizationType)
            writer.Append(')');
        
        writer.Append(", ");

        if (IsVectorBase64Encoded)
            writer.Append("base64(");

        if (QueriedVectorQuantizationType != EmbeddingType.Float32)
            writer.Append($"{QueriedVectorQuantizationType.ToString().ToLower()}(");
        
        writer.Append($"${ParameterName}");

        if (QueriedVectorQuantizationType != EmbeddingType.Float32)
            writer.Append(')');

        if (IsVectorBase64Encoded)
            writer.Append(')');

        writer.Append($", {SimilarityThreshold}");
        
        writer.Append(')');
    }
}
