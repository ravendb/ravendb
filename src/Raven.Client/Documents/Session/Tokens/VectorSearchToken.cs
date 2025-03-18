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
    private readonly int? _numberOfCandidatesForQuerying;
    public VectorSearchToken(string fieldName, string parameterName, VectorEmbeddingType sourceQuantizationType, VectorEmbeddingType targetQuantizationType, float? similarityThreshold, int? numberOfCandidatesForQuerying, bool isExact)
    {
        FieldName = fieldName;
        ParameterName = parameterName;
        
        _sourceQuantizationType = sourceQuantizationType;
        _targetQuantizationType = targetQuantizationType;

        _similarityThreshold = similarityThreshold;

        _numberOfCandidatesForQuerying = numberOfCandidatesForQuerying;
        Options = new(isExact);
    }
    
    public override void WriteTo(StringBuilder writer)
    {
        if (Options.Boost.HasValue)
            writer.Append("boost(");
            
        if (Options.Exact)
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

        if (Options.Exact)
            writer.Append(')');
        
        if (Options.Boost.HasValue)
            writer.Append($", {Options.Boost.Value.ToInvariantString()})");
    }
}
