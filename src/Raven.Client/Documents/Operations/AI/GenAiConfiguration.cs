using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.ETL;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

/// <summary>
/// ETL configuration for running GenAI processing (chat/completions) over documents in a collection.
/// </summary>
public class GenAiConfiguration : AbstractAiIntegrationConfiguration
{
    /// <inheritdoc />
    public override string GetDestination() => Identifier;
    /// <inheritdoc />
    public override string GetDefaultTaskName() => Identifier;

    /// <summary>
    /// The identifier used to group and track GenAI tasks associated with this configuration.
    /// </summary>
    public string Identifier { get; set; }

    /// <summary>
    /// The source collection on which the GenAI ETL will operate.
    /// </summary>
    public string Collection { get; set; }
    
    /// <summary>
    /// The ETL type. Always <see cref="EtlType.GenAi"/> for this configuration.
    /// </summary>
    public override EtlType EtlType => EtlType.GenAi;

    /// <summary>
    /// Indicates whether the underlying provider connection uses HTTPS or other encrypted transport.
    /// </summary>
    public override bool UsingEncryptedCommunicationChannel() => Connection?.UsingEncryptedCommunicationChannel() ?? false;

    /// <summary>
    /// Generates a normalized identifier from the configuration name.
    /// </summary>
    public string GenerateIdentifier() => EmbeddingsGenerationConfiguration.GenerateIdentifier(Name);

    /// <summary>
    /// The transformation script/settings used to perform the GenAI processing.
    /// </summary>
    public GenAiTransformation GenAiTransformation { get; set; }

    /// <summary>
    /// Optional prompt template sent to the model.
    /// </summary>
    public string Prompt { get; set; }
    
    //TODO: Make this JSON objects? 
    /// <summary>Optional JSON schema for the model output.</summary>
    public string JsonSchema { get; set; }
    /// <summary>Optional sample object demonstrating expected output shape.</summary>
    public string SampleObject { get; set; }
    /// <summary>JavaScript update function that applies results back to documents.</summary>
    public string UpdateScript { get; set; }

    /// <summary>
    /// The maximum number of documents processed concurrently by the task.
    /// </summary>
    public int MaxConcurrency { get; set; } = DefaultMaxConcurrency;


    private List<Transformation> _transforms;

    private const int DefaultMaxConcurrency = 4;

    /// <summary>
    /// Not supported for GenAI; use <see cref="GenAiTransformation"/> instead.
    /// </summary>
    [JsonDeserializationIgnore]
    [JsonIgnore]
    [Obsolete($"{nameof(GenAiConfiguration)} doesn't support multiple transformations. Please use {nameof(GenAiTransformation)} property instead.")]
#pragma warning disable CS0809 // Obsolete member overrides non-obsolete member
    public override List<Transformation> Transforms
#pragma warning restore CS0809 // Obsolete member overrides non-obsolete member
    {
        get
        {
            return _transforms ??=
            [
                new Transformation
                {
                    Name = "GenAi-transform-script",
                    Collections = [Collection],
                    Script = GenAiTransformation?.Script
                }
            ];
        }
        set
        {
            throw new NotSupportedException($"{nameof(GenAiConfiguration)} doesn't support multiple transformations. Please use {nameof(GenAiTransformation)} property instead.");
        }
    }

    /// <summary>
    /// Validates the configuration and optionally its connection/name/identifier.
    /// </summary>
    public override bool Validate(out List<string> errors, bool validateName = true, bool validateConnection = true, bool validateIdentifier = true)
    {
        if (validateConnection && Initialized == false)
            throw new InvalidOperationException("GenAi configuration must be initialized");

        errors = [];

        if (validateIdentifier && AiTaskIdentifierHelper.ValidateIdentifier(Identifier, out var idErrors) == false)
            errors.AddRange(idErrors);

        if (validateName && string.IsNullOrEmpty(Name))
            errors.Add($"{nameof(Name)} of GenAi configuration cannot be empty");

        if (TestMode == false && string.IsNullOrEmpty(ConnectionStringName))
            errors.Add($"{nameof(ConnectionStringName)} cannot be empty");

        if (validateConnection && TestMode == false)
        {
            Connection.Validate(errors);
        }

        if (validateConnection)
        {
            if (Connection.ModelType != AiModelType.Chat)
                errors.Add($"{nameof(Connection.ModelType)} of GenAI configuration must be {nameof(AiModelType.Chat)}");
        }

        if (string.IsNullOrEmpty(Collection))
            errors.Add($"{nameof(Collection)} must be provided");

        if (GenAiTransformation == null)
            errors.Add($"{nameof(GenAiTransformation)} must be specified");

        else if (GenAiTransformation.ValidateScript(out var error) == false)
            errors.Add(error);

        if (TestMode == false && string.IsNullOrEmpty(UpdateScript))
            errors.Add("You must provide an update function");

        return errors.Count == 0;
    }

    /// <summary>
    /// Serializes this configuration to JSON.
    /// </summary>
    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();

        json[nameof(Identifier)] = Identifier;
        json[nameof(AiConnectorType)] = AiConnectorType;
        json[nameof(Identifier)] = Identifier;
        json[nameof(Collection)] = Collection;
        json[nameof(Prompt)] = Prompt;
        json[nameof(SampleObject)] = SampleObject;
        json[nameof(JsonSchema)] = JsonSchema;
        json[nameof(UpdateScript)] = UpdateScript;
        json[nameof(GenAiTransformation)] = GenAiTransformation.ToJson();
        json[nameof(MaxConcurrency)] = MaxConcurrency;

        return json;
    }

    internal override EtlConfigurationCompareDifferences Compare(EtlConfiguration<AiConnectionString> config, Dictionary<string, AiConnectionString> connectionStrings, List<(string TransformationName, EtlConfigurationCompareDifferences Difference)> transformationDiffs = null)
    {
        var differences = base.Compare(config, connectionStrings, transformationDiffs);
        if (config is not GenAiConfiguration other)
            return differences;

        if (Prompt != other.Prompt ||
            UpdateScript != other.UpdateScript ||
            JsonSchema != other.JsonSchema ||
            SampleObject != other.SampleObject ||
            MaxConcurrency != other.MaxConcurrency)
            differences |= EtlConfigurationCompareDifferences.Other;

        return differences;
    }
}
