using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.ETL;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

public sealed class AiIntegrationConfiguration : EtlConfiguration<AiConnectionString>
{
    public string Identifier { get; set; }

    [JsonDeserializationIgnore]
    [JsonIgnore]
    public AiConnectorType AiConnectorType => Connection?.GetActiveProvider() ?? AiConnectorType.None;

    public override string GetDestination() => Identifier;
    public override string GetDefaultTaskName() => Identifier;

    public override EtlType EtlType => EtlType.Ai;

    public string Collection { get; set; }

    public List<string> EmbeddingsPaths { get; set; }

    public AiEmbeddingsTransformation EmbeddingsTransformation { get; set; }
    
    public VectorEmbeddingType TargetQuantizationType { get; set; }

    private List<Transformation> _transforms;

    [JsonDeserializationIgnore]
    [JsonIgnore]
    [Obsolete($"AI Integration configuration doesn't support multiple transformations. Please use {nameof(EmbeddingsTransformation)} property instead.")]
#pragma warning disable CS0809 // Obsolete member overrides non-obsolete member
    public override List<Transformation> Transforms
#pragma warning restore CS0809 // Obsolete member overrides non-obsolete member
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
        
        if (TargetQuantizationType == VectorEmbeddingType.Text)
            errors.Add($"{nameof(TargetQuantizationType)} cannot be {nameof(VectorEmbeddingType.Text)}");

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

        json[nameof(Identifier)] = Identifier;
        json[nameof(Collection)] = Collection;
        json[nameof(EmbeddingsPaths)] = new DynamicJsonArray(EmbeddingsPaths);
        json[nameof(EmbeddingsTransformation)] = EmbeddingsTransformation != null ? new DynamicJsonValue
        {
            [nameof(EmbeddingsTransformation.Script)] = EmbeddingsTransformation.Script
        } : null;
        json[nameof(AiConnectorType)] = AiConnectorType;
        json[nameof(TargetQuantizationType)] = TargetQuantizationType;

        return json;
    }
    internal string GenerateIdentifier() => GenerateIdentifier(Name);

    internal bool ValidateIdentifier(out List<string> errors)
    {
        return ValidateIdentifier(Identifier, out errors);
    }

    internal static string GenerateIdentifier(string input)
    {
        if (string.IsNullOrWhiteSpace(input))
            return null;

        var result = new StringBuilder();
        var lastWasHyphen = false;

        // First normalize to FormD to separate letters from their diacritics
        foreach (var c in input.Normalize(NormalizationForm.FormD))
        {
            // Check if this is a letter that needs to be preserved
            if (c is
                >= 'a' and <= 'z' or
                >= '0' and <= '9')
            {
                result.Append(c);
                lastWasHyphen = false;
            }
            else if (c is >= 'A' and <= 'Z')
            {
                result.Append(char.ToLowerInvariant(c));
                lastWasHyphen = false;
            }
            else if (lastWasHyphen == false && result.Length > 0) // Add hyphen for any other character
            {
                result.Append('-');
                lastWasHyphen = true;
            }
        }

        // Trim any trailing hyphens
        var finalResult = result.ToString().TrimEnd('-');

        // Ensure we have at least one character
        return string.IsNullOrEmpty(finalResult) ? $"{nameof(AiConnectionString)}Identifier" : finalResult;
    }

    internal static bool ValidateIdentifier(string identifier, out List<string> errors)
    {
        errors = [];

        if (string.IsNullOrWhiteSpace(identifier))
        {
            errors.Add("Identifier cannot be empty or contain only whitespace;");
            return false;
        }

        // Check that the string is already normalized (contains only a-z, 0-9 and hyphens)
        if (identifier != identifier.Normalize(NormalizationForm.FormD))
            errors.Add("Identifier contains diacritical marks or non-ASCII characters;");

        // Check that there are no uppercase letters
        if (identifier.Any(char.IsUpper))
            errors.Add("Identifier contains uppercase letters;");

        // Check for invalid characters and collect them
        var invalidChars = identifier.Where(c => c is not (>= 'a' and <= 'z' or >= '0' and <= '9' or '-'))
            .Distinct()
            .ToList();
        if (invalidChars.Count != 0)
            errors.Add($"Identifier contains invalid characters: {string.Join(", ", invalidChars.Select(c => $"'{c}'"))}. " +
                       $"Only lowercase letters (a-z), numbers (0-9) and hyphens (-) are allowed.");

        // Check that there are no consecutive hyphens
        if (identifier.Contains("--"))
            errors.Add("Identifier contains consecutive hyphens;");

        // Check that the string does not end with a hyphen
        if (identifier.EndsWith("-"))
            errors.Add("Identifier ends with a hyphen;");

        return errors.Count != 0 == false;
    }
}
