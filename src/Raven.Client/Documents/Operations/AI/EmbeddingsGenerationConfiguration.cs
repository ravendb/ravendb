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

/// <summary>
/// ETL configuration for generating vector embeddings from documents in a collection.
/// </summary>
public sealed class EmbeddingsGenerationConfiguration : AbstractAiIntegrationConfiguration
{
    /// <summary>
    /// The identifier used to group and track embeddings tasks associated with this configuration.
    /// </summary>
    public string Identifier { get; set; }
    /// <inheritdoc />
    public override string GetDestination() => Identifier;
    /// <inheritdoc />
    public override string GetDefaultTaskName() => Identifier;

    /// <summary>
    /// The ETL type. Always <see cref="EtlType.EmbeddingsGeneration"/> for this configuration.
    /// </summary>
    public override EtlType EtlType => EtlType.EmbeddingsGeneration;

    /// <summary>
    /// The source collection on which the embeddings ETL will operate.
    /// </summary>
    public string Collection { get; set; }

    /// <summary>
    /// Path-based configuration describing where to extract text and how to chunk it.
    /// Mutually exclusive with providing a custom <see cref="EmbeddingsTransformation"/> script.
    /// </summary>
    public List<EmbeddingPathConfiguration> EmbeddingsPathConfigurations { get; set; }

    /// <summary>
    /// Custom transformation that generates embeddings via JavaScript. If provided,
    /// it is used instead of <see cref="EmbeddingsPathConfigurations"/>.
    /// </summary>
    public EmbeddingsTransformation EmbeddingsTransformation { get; set; }

    /// <summary>
    /// The embedding vector type or quantization setting to use for storage.
    /// </summary>
    public VectorEmbeddingType Quantization { get; set; }

    /// <summary>
    /// Chunking behavior used when splitting query text for vector search.
    /// </summary>
    public ChunkingOptions ChunkingOptionsForQuerying { get; set; }

    /// <summary>
    /// Time-to-live for cached embeddings generated from documents.
    /// </summary>
    public TimeSpan EmbeddingsCacheExpiration { get; set; } = TimeSpan.FromDays(90);

    /// <summary>
    /// Time-to-live for cached embeddings generated for querying.
    /// </summary>
    public TimeSpan EmbeddingsCacheForQueryingExpiration { get; set; } = TimeSpan.FromDays(14);

    private const string PathsTransformationName = "embeddings-from-paths";
    private const string ScriptTransformationName = "embeddings-transform-script";

    internal string TransformationName => EmbeddingsTransformation == null ? PathsTransformationName : ScriptTransformationName;

    private List<Transformation> _transforms;

    /// <summary>
    /// Not supported for embeddings ETL; use <see cref="EmbeddingsTransformation"/> or <see cref="EmbeddingsPathConfigurations"/>.
    /// </summary>
    [JsonDeserializationIgnore]
    [JsonIgnore]
#if NETCOREAPP3_1_OR_GREATER
    [System.Text.Json.Serialization.JsonIgnore]
#endif
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
                        Name = PathsTransformationName,
                        Collections = [Collection]
                    }
                };

            return _transforms ??=
            [
                new Transformation
                {
                    Name = ScriptTransformationName,
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

    /// <summary>
    /// Validates the configuration and optionally its connection/name/identifier.
    /// </summary>
    public override bool Validate(out List<string> errors, bool validateName = true, bool validateConnection = true, bool validateIdentifier = true, EtlConfiguration<AiConnectionString> existingConfiguration = null)
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

        if (EmbeddingsPathConfigurations is not null)
        {
            foreach (var pathConfiguration in EmbeddingsPathConfigurations)
            {
                if (pathConfiguration.ChunkingOptions is not null)
                    pathConfiguration.ChunkingOptions.Validate(pathConfiguration.Path, errors);
                else
                    errors.Add($"Path '{pathConfiguration.Path}': {nameof(ChunkingOptions)} must be provided.");
            }
        }

        EmbeddingsTransformation?.Validate(errors);
        
        if (Quantization == VectorEmbeddingType.Text)
            errors.Add($"{nameof(Quantization)} cannot be {nameof(VectorEmbeddingType.Text)}");

        if (ChunkingOptionsForQuerying is null)
        {
            errors.Add($"{nameof(ChunkingOptionsForQuerying)} must be provided.");
        }
        else
        {
            if (ChunkingOptionsForQuerying.MaxTokensPerChunk <= 0)
                errors.Add($"{nameof(ChunkingOptionsForQuerying)} must be specified with {nameof(ChunkingOptionsForQuerying.MaxTokensPerChunk)} greater than 0.");
        
            if (ChunkingOptionsForQuerying.OverlapTokens < 0)
                errors.Add($"{nameof(ChunkingOptionsForQuerying)} must be specified with {nameof(ChunkingOptionsForQuerying.OverlapTokens)} greater than, or equal to 0.");
        }

        return errors.Count == 0;
    }

    /// <summary>
    /// Indicates whether the underlying provider connection uses HTTPS or other encrypted transport.
    /// </summary>
    public override bool UsingEncryptedCommunicationChannel()
    {
        return Connection?.UsingEncryptedCommunicationChannel() ?? false;
    }

    /// <summary>
    /// Serializes this configuration to JSON.
    /// </summary>
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
                [nameof(ChunkingOptionsForQuerying.OverlapTokens)] = EmbeddingsTransformation.ChunkingOptions.OverlapTokens,
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
        return AiTaskIdentifierHelper.ValidateIdentifier(Identifier, out errors);
    }

    internal static string GenerateIdentifier(string input)
    {
       return AiTaskIdentifierHelper.GenerateIdentifier(input);
    }
    
    internal override EtlConfigurationCompareDifferences Compare(EtlConfiguration<AiConnectionString> config, Dictionary<string, AiConnectionString> connectionStrings, List<(string TransformationName, EtlConfigurationCompareDifferences Difference)> transformationDiffs = null)
    {
        var differences = base.Compare(config, connectionStrings, transformationDiffs);
        if (config is not EmbeddingsGenerationConfiguration other)
            return differences;

        if (Collection != other.Collection ||
            Quantization != other.Quantization ||
            EmbeddingsCacheExpiration != other.EmbeddingsCacheExpiration ||
            EmbeddingsCacheForQueryingExpiration != other.EmbeddingsCacheForQueryingExpiration ||
            EmbeddingsTransformation.AreEqual(EmbeddingsTransformation, other.EmbeddingsTransformation) == false ||
            ChunkingOptions.AreEqual(ChunkingOptionsForQuerying,other.ChunkingOptionsForQuerying) == false)
            differences |= EtlConfigurationCompareDifferences.Other;
        
        differences |= CompareEmbeddingsPathConfigurations(other.EmbeddingsPathConfigurations);

        return differences;
    }

    private EtlConfigurationCompareDifferences CompareEmbeddingsPathConfigurations(List<EmbeddingPathConfiguration> other)
    {
        if (EmbeddingsPathConfigurations == null && 
            other == null)
            return EtlConfigurationCompareDifferences.None;
        
        if (EmbeddingsPathConfigurations == null ||
            other == null)
            return EtlConfigurationCompareDifferences.Other;
        
        if (EmbeddingsPathConfigurations.Count != other.Count)
            return EtlConfigurationCompareDifferences.Other;
        
        foreach (var pathConfiguration in EmbeddingsPathConfigurations)
        {
            var otherPathConfiguration = other.SingleOrDefault(x => x.Path == pathConfiguration.Path);

            if (otherPathConfiguration == null ||
                EmbeddingPathConfiguration.AreEqual(pathConfiguration, otherPathConfiguration) == false)
                return EtlConfigurationCompareDifferences.Other;
        }

        return EtlConfigurationCompareDifferences.None;
    }
}
