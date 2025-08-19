using System;
using System.IO;
using System.Text;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Queries;
using Raven.Client.Extensions;
using Sparrow;
using Sparrow.Extensions;

namespace Raven.Client.Documents.Session.Tokens;

public sealed class VectorSearchToken : WhereToken
{
    private readonly float? _similarityThreshold;
    private readonly VectorEmbeddingType _sourceQuantizationType;
    private readonly VectorEmbeddingType _targetQuantizationType;
    private readonly int? _numberOfCandidatesForQuerying;
    private readonly bool _isDocumentId;
    private readonly string _embeddingsGenerationTaskIdentifier;
    private readonly string _embeddingsGenerationTaskIdentifierByValue;
    
    public VectorSearchToken(string fieldName, string parameterName, VectorEmbeddingType sourceQuantizationType, VectorEmbeddingType targetQuantizationType, float? similarityThreshold, int? numberOfCandidatesForQuerying, bool isExact, bool isDocumentId, string embeddingsGenerationTaskIdentifier, string embeddingsGenerationTaskIdentifierByValue = null)
    {
        FieldName = fieldName;
        ParameterName = parameterName;
        
        _sourceQuantizationType = sourceQuantizationType;
        _targetQuantizationType = targetQuantizationType;

        _similarityThreshold = similarityThreshold;

        _numberOfCandidatesForQuerying = numberOfCandidatesForQuerying;
        _isDocumentId = isDocumentId;
        Options = new(isExact);

        _embeddingsGenerationTaskIdentifier = embeddingsGenerationTaskIdentifier;
        _embeddingsGenerationTaskIdentifierByValue = embeddingsGenerationTaskIdentifierByValue;
        
        PortableExceptions.ThrowIf<InvalidOperationException>(embeddingsGenerationTaskIdentifier != null && embeddingsGenerationTaskIdentifierByValue != null, $"Embeddings generation task identifier set in value factory cannot be used with field factory. It solely purpose to use already generated embeddings.");
    }
    
    public override void WriteTo(StringBuilder writer)
    {
        if (Options.Boost.HasValue)
            writer.Append("boost(");
            
        if (Options.Exact)
            writer.Append("exact(");
        
        writer.Append("vector.search(");

        if (_sourceQuantizationType is VectorEmbeddingType.Single && _targetQuantizationType is VectorEmbeddingType.Single)
        {
            writer.Append(FieldName);
        }
        else
        {
            var methodName = Constants.VectorSearch.ConfigurationToMethodName(_sourceQuantizationType, _targetQuantizationType);
            
            if (_sourceQuantizationType is VectorEmbeddingType.Text && _embeddingsGenerationTaskIdentifier != null)
                writer.Append($"{methodName}({FieldName},{Constants.VectorSearch.AiTaskMethodName}('{_embeddingsGenerationTaskIdentifier}'))");
            else
                writer.Append($"{methodName}({FieldName})");
        }
        writer.Append(", ");

        if (_isDocumentId)
        {
            writer.Append($"{Constants.VectorSearch.EmbeddingForDocument}(${ParameterName})");
        }
        else if (_embeddingsGenerationTaskIdentifierByValue != null)
        {
            //embedding.text('textual input', ai.task('task-name'))
            writer.Append($"{Constants.VectorSearch.EmbeddingText}(${ParameterName}, {Constants.VectorSearch.AiTaskMethodName}('{_embeddingsGenerationTaskIdentifierByValue}'))");
        }
        else
        {
            writer.Append($"${ParameterName}");
        }

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
