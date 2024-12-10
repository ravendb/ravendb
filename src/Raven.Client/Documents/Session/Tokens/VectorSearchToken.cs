using System;
using System.IO;
using System.Text;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Queries;
using Raven.Client.Extensions;
using Sparrow.Extensions;

namespace Raven.Client.Documents.Session.Tokens;

public sealed class VectorSearchToken : WhereToken
{
    private readonly float? _similarityThreshold;
    private readonly VectorEmbeddingType _sourceQuantizationType;
    private readonly VectorEmbeddingType _targetQuantizationType;
    private bool _isSourceBase64Encoded;
    private bool _isVectorBase64Encoded;
    private readonly int? _numberOfCandidatesForQuerying;
    private bool _isExact;
    
    public VectorSearchToken(string fieldName, string parameterName, VectorEmbeddingType sourceQuantizationType, VectorEmbeddingType targetQuantizationType, bool isSourceBase64Encoded, bool isVectorBase64Encoded, float? similarityThreshold, int? numberOfCandidatesForQuerying, bool isExact)
    {
        FieldName = fieldName;
        ParameterName = parameterName;
        
        _sourceQuantizationType = sourceQuantizationType;
        _targetQuantizationType = targetQuantizationType;
                
        _isSourceBase64Encoded = isSourceBase64Encoded;
        _isVectorBase64Encoded = isVectorBase64Encoded;
                
        _similarityThreshold = similarityThreshold;

        _numberOfCandidatesForQuerying = numberOfCandidatesForQuerying;
        _isExact = isExact;
    }
    
    public override void WriteTo(StringBuilder writer)
    {
        if (_isExact)
            writer.Append("exact(");
        
        writer.Append("vector.search(");
        
        if (_sourceQuantizationType is VectorEmbeddingType.Single && _targetQuantizationType is VectorEmbeddingType.Single)
            writer.Append(FieldName);
        else
        {
            var methodName = Constants.VectorSearch.ConfigurationToMethodName(_sourceQuantizationType, _targetQuantizationType);
            writer.Append($"{methodName}({FieldName})");
        }
        
        writer.Append($", ${ParameterName}");

        bool parametersAreDefault = _similarityThreshold is null &&
                                    _numberOfCandidatesForQuerying is null;

        if (parametersAreDefault == false)
        {
            writer.Append($", {_similarityThreshold?.ToInvariantString() ?? "null"}");
            writer.Append($", {_numberOfCandidatesForQuerying?.ToInvariantString() ?? "null" }");
        }
        
        writer.Append(')');

        if (_isExact)
            writer.Append(')');
    }
}
