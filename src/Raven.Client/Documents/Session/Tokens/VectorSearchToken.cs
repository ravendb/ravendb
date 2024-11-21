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
    private VectorEmbeddingType SourceQuantizationType { get; set; }
    private VectorEmbeddingType TargetQuantizationType { get; set; }
    private bool IsSourceBase64Encoded { get; set; }
    private bool IsVectorBase64Encoded { get; set; }
    private int NumberOfCandidatesForQuerying { get; set; }
    
    public VectorSearchToken(string fieldName, string parameterName, VectorEmbeddingType sourceQuantizationType, VectorEmbeddingType targetQuantizationType, bool isSourceBase64Encoded, bool isVectorBase64Encoded, float similarityThreshold, int numberOfCandidatesForQuerying)
    {
        FieldName = fieldName;
        ParameterName = parameterName;
        
        SourceQuantizationType = sourceQuantizationType;
        TargetQuantizationType = targetQuantizationType;
                
        IsSourceBase64Encoded = isSourceBase64Encoded;
        IsVectorBase64Encoded = isVectorBase64Encoded;
                
        SimilarityThreshold = similarityThreshold;

        NumberOfCandidatesForQuerying = numberOfCandidatesForQuerying;
    }
    
    public override void WriteTo(StringBuilder writer)
    {
        writer.Append("vector.search(");
        
        if (SourceQuantizationType is VectorEmbeddingType.Single && TargetQuantizationType is VectorEmbeddingType.Single)
            writer.Append(FieldName);
        else
        {
            var methodName = Constants.VectorSearch.ConfigurationToMethodName(SourceQuantizationType, TargetQuantizationType);
            writer.Append($"{methodName}({FieldName})");
        }
        
        writer.Append($", ${ParameterName}");

        if (SimilarityThreshold.AlmostEquals(Constants.VectorSearch.DefaultMinimumSimilarity) == false)
            writer.Append($", {SimilarityThreshold}");

        if (NumberOfCandidatesForQuerying != Constants.VectorSearch.DefaultNumberOfCandidatesForQuerying)
            writer.Append($", {NumberOfCandidatesForQuerying}");
        
        writer.Append(')');
    }
}
