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

public sealed class EmbeddingsGenerationConfiguration : AbstractAiIntegrationConfiguration
{
    public string Identifier { get; set; }

    public override string GetDestination() => Identifier;
    public override string GetDefaultTaskName() => Identifier;

    public override EtlType EtlType => EtlType.EmbeddingsGeneration;

    public string Collection { get; set; }

    public List<EmbeddingPathConfiguration> EmbeddingsPathConfigurations { get; set; }

    public EmbeddingsTransformation EmbeddingsTransformation { get; set; }
    
    public VectorEmbeddingType Quantization { get; set; }
    
    public ChunkingOptions ChunkingOptionsForQuerying { get; set; }

    public TimeSpan EmbeddingsCacheExpiration { get; set; } = TimeSpan.FromDays(90);

    public TimeSpan EmbeddingsCacheForQueryingExpiration { get; set; } = TimeSpan.FromDays(14);

    private List<Transformation> _transforms;

    [JsonDeserializationIgnore]
    [JsonIgnore]
    [Obsolete($"{nameof(EmbeddingsGenerationConfiguration)} doesn't support multiple transformations. Please use {nameof(EmbeddingsTransformation)} property instead.")]
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
            throw new NotSupportedException($"{nameof(EmbeddingsGenerationConfiguration)} doesn't support multiple transformations. Please use {nameof(EmbeddingsTransformation)} property instead.");
        }
    }

    public override bool Validate(out List<string> errors, bool validateName = true, bool validateConnection = true, bool validateIdentifier = true)
    {
        if (validateConnection && Initialized == false)
            throw new InvalidOperationException("Embeddings Generation configuration must be initialized");

        errors = [];

        if (validateIdentifier && AiTaskIdentifierHelper.ValidateIdentifier(Identifier, out var idErrors) == false)
            errors.AddRange(idErrors);

        if (validateName && string.IsNullOrEmpty(Name))
            errors.Add($"{nameof(Name)} of Embeddings Generation configuration cannot be empty");

        if (TestMode == false && string.IsNullOrEmpty(ConnectionStringName))
            errors.Add($"{nameof(ConnectionStringName)} cannot be empty");

        if (validateConnection && TestMode == false)
        {
            Connection.Validate(errors);
        }

        if (validateConnection)
        {
            if (Connection.ModelType != AiModelType.TextEmbeddings)
                errors.Add($"{nameof(Connection.ModelType)} of Embeddings Generation configuration must be {nameof(AiModelType.TextEmbeddings)}");
        }

        if (string.IsNullOrEmpty(Collection))
            errors.Add($"{nameof(Collection)} must be provided");

        if ((EmbeddingsPathConfigurations is null || EmbeddingsPathConfigurations.Count == 0) &&
            (EmbeddingsTransformation is null || string.IsNullOrEmpty(EmbeddingsTransformation.Script)))
        {
            errors.Add($"Configuration must have either {nameof(EmbeddingsPathConfigurations)} or {nameof(EmbeddingsTransformation)} script specified");
        }

        if (EmbeddingsTransformation?.ValidateScript() == false)
            errors.Add($"Transformation script must use {EmbeddingsTransformation.GenerateEmbeddingsFunctionName} method.");
        
        if (Quantization == VectorEmbeddingType.Text)
            errors.Add($"{nameof(Quantization)} cannot be {nameof(VectorEmbeddingType.Text)}");

        if (ChunkingOptionsForQuerying is null || ChunkingOptionsForQuerying.MaxTokensPerChunk <= 0)
            errors.Add($"{nameof(ChunkingOptionsForQuerying)} must be specified with {nameof(ChunkingOptionsForQuerying.MaxTokensPerChunk)} greater than 0.");

        return errors.Count == 0;
    }

    public override bool UsingEncryptedCommunicationChannel()
    {
        return Connection?.UsingEncryptedCommunicationChannel() ?? false;
    }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();

        json[nameof(Identifier)] = Identifier;
        json[nameof(Collection)] = Collection;
        json[nameof(EmbeddingsPathConfigurations)] = new DynamicJsonArray(EmbeddingsPathConfigurations);
        json[nameof(EmbeddingsTransformation)] = EmbeddingsTransformation != null ? new DynamicJsonValue
        {
            [nameof(EmbeddingsTransformation.Script)] = EmbeddingsTransformation.Script,
            [nameof(EmbeddingsTransformation.ChunkingOptions)] = new DynamicJsonValue()
            {
                [nameof(ChunkingOptionsForQuerying.ChunkingMethod)] = EmbeddingsTransformation.ChunkingOptions.ChunkingMethod,
                [nameof(ChunkingOptionsForQuerying.MaxTokensPerChunk)] = EmbeddingsTransformation.ChunkingOptions.MaxTokensPerChunk,
            }
        } : null;
        json[nameof(AiConnectorType)] = AiConnectorType;
        json[nameof(Quantization)] = Quantization;
        json[nameof(EmbeddingsCacheExpiration)] = EmbeddingsCacheExpiration;
        json[nameof(ChunkingOptionsForQuerying)] = ChunkingOptionsForQuerying;
        json[nameof(EmbeddingsCacheForQueryingExpiration)] = EmbeddingsCacheForQueryingExpiration;

        return json;
    }
    internal string GenerateIdentifier() => GenerateIdentifier(Name);

    internal bool ValidateIdentifier(out List<string> errors)
    {
        return ValidateIdentifier(Identifier, out errors);
    }

    internal static string GenerateIdentifier(string input)
    {
       return AiTaskIdentifierHelper.GenerateIdentifier(input);
    }

    internal static bool ValidateIdentifier(string identifier, out List<string> errors)
    {
        return AiTaskIdentifierHelper.ValidateIdentifier(identifier, out errors);
    }
}
