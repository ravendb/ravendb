using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ETL;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

public sealed class AiIntegrationConfiguration : EtlConfiguration<AiConnectionString>
{

    private string _name;
    private string Identifier => _name ??= Connection?.Name;

    public AiConnectorType AiConnectorType => Connection?.GetActiveProvider() ?? AiConnectorType.None;

    public List<string> PathsToProcess { get; set; }

    public override string GetDestination() => Identifier;
    public override string GetDefaultTaskName() => Identifier;

    public override EtlType EtlType => EtlType.Ai;

    private string _normalizedConnectionName;

    public string NormalizedConnectionName
    {
        get
        {
            _normalizedConnectionName ??= string.Join(string.Empty, ConnectionStringName.ToLowerInvariant().Where(x => char.IsWhiteSpace(x) == false));
            return _normalizedConnectionName;
        }
    }

    public string Collection { get; set; }

    public List<string> EmbeddingsPaths { get; set; }

    public AiEmbeddingsTransformation EmbeddingsTransformation { get; set; }

    private List<Transformation> _transforms;

    [JsonDeserializationIgnore]
    [JsonIgnore]
    [Obsolete($"AI Integration configuration doesn't support multiple transformations. Please use {nameof(EmbeddingsTransformation)} property instead.")]
    public override List<Transformation> Transforms
    {
        get
        {
            if (EmbeddingsTransformation == null)
                return _transforms ??= new List<Transformation>()
                {
                    new Transformation
                    {
                        Name = "embeddings-from-paths",
                        Collections = [Collection]
                    }
                };

            return _transforms ??=
            [
                new Transformation
                {
                    Name = "embeddings-transform-script",
                    Collections = [Collection],
                    Script = EmbeddingsTransformation.Script
                }
            ];
        }
        set
        {
            throw new NotSupportedException($"AI Integration configuration doesn't support multiple transformations. Please use {nameof(EmbeddingsTransformation)} property instead.");
        }
    }

    public override bool Validate(out List<string> errors, bool validateName = true, bool validateConnection = true)
    {
        if (validateConnection && Initialized == false)
            throw new InvalidOperationException("AI Integration configuration must be initialized");

        errors = new List<string>();

        if (validateName && string.IsNullOrEmpty(Name))
            errors.Add($"{nameof(Name)} of AI Integration configuration cannot be empty");

        if (TestMode == false && string.IsNullOrEmpty(ConnectionStringName))
            errors.Add($"{nameof(ConnectionStringName)} cannot be empty");

        if (validateConnection && TestMode == false)
            Connection.Validate(ref errors);

        if (string.IsNullOrEmpty(Collection))
            errors.Add($"{nameof(Collection)} must be provided");

        if ((EmbeddingsPaths is null || EmbeddingsPaths.Count == 0) &&
            (EmbeddingsTransformation is null || string.IsNullOrEmpty(EmbeddingsTransformation.Script)))
        {
            errors.Add($"Configuration must have either {nameof(EmbeddingsPaths)} or {nameof(EmbeddingsTransformation)} script specified");
        }

        return errors.Count == 0;
    }

    public override bool UsingEncryptedCommunicationChannel()
    {
        switch (AiConnectorType)
        {
            case AiConnectorType.Ollama:
                return Connection.OllamaSettings.Uri.StartsWith("https");
            case AiConnectorType.OpenAi:
                return Connection.OpenAiSettings.Endpoint.StartsWith("https");
            case AiConnectorType.AzureOpenAi:
                return Connection.AzureOpenAiSettings.Endpoint.StartsWith("https");
            case AiConnectorType.MistralAi:
                return Connection.MistralAiSettings.Endpoint.StartsWith("https");
            case AiConnectorType.HuggingFace:
                // Endpoint is optional for HuggingFace, it will use the default endpoint if not provided, which is HTTPS
                return string.IsNullOrWhiteSpace(Connection.HuggingFaceSettings.Endpoint) || Connection.HuggingFaceSettings.Endpoint.StartsWith("https");
            case AiConnectorType.Onnx:
            case AiConnectorType.Google:
                return true;

            default:
                throw new NotSupportedException($"Unknown AI connector type: {AiConnectorType}");
        }
    }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();

        json[nameof(EmbeddingsPaths)] = new DynamicJsonArray(EmbeddingsPaths);
        json[nameof(AiConnectorType)] = AiConnectorType;

        return json;
    }
}
