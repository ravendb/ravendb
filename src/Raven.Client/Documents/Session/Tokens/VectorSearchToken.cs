using System.Text;
using Raven.Client.Documents.Queries;

namespace Raven.Client.Documents.Session.Tokens;

public sealed class VectorSearchToken : WhereToken
{
    private float SimilarityThreshold { get; set; }
    private EmbeddingQuantizationType SourceQuantizationType { get; set; }
    private EmbeddingQuantizationType TargetQuantizationType { get; set; }
    private EmbeddingQuantizationType QueriedVectorQuantizationType { get; set; }
    private bool IsSourceBase64Encoded { get; set; }
    private bool IsVectorBase64Encoded { get; set; }
    
    public VectorSearchToken(string fieldName, string parameterName, EmbeddingQuantizationType sourceQuantizationType, EmbeddingQuantizationType targetQuantizationType, EmbeddingQuantizationType queriedQueriedVectorQuantizationType, bool isSourceBase64Encoded, bool isVectorBase64Encoded, float similarityThreshold)
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
        
        if (SourceQuantizationType == EmbeddingQuantizationType.None)
        {
            if (TargetQuantizationType != EmbeddingQuantizationType.None)
            {
                // TODO
                writer.Append($"f32_{TargetQuantizationType.ToString().ToLower()}(");

                explicitSourceQuantizationType = true;
            }
        }
        
        else if (TargetQuantizationType != EmbeddingQuantizationType.None)
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

        if (QueriedVectorQuantizationType != EmbeddingQuantizationType.F32)
            writer.Append($"{QueriedVectorQuantizationType.ToString().ToLower()}(");
        
        writer.Append($"${ParameterName}");

        if (QueriedVectorQuantizationType != EmbeddingQuantizationType.F32)
            writer.Append(')');

        if (IsVectorBase64Encoded)
            writer.Append(')');

        writer.Append($", {SimilarityThreshold}");
        
        writer.Append(')');
    }
}
