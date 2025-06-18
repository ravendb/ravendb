using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.ETL;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI;

public class GenAiConfiguration : AbstractAiIntegrationConfiguration
{
    public override string GetDestination() => Identifier;
    public override string GetDefaultTaskName() => Identifier;

    public string Identifier { get; set; }
    public string Collection { get; set; }
    
    public override EtlType EtlType => EtlType.GenAi;
    public override bool UsingEncryptedCommunicationChannel() => Connection?.UsingEncryptedCommunicationChannel() ?? false;

    public string GenerateIdentifier() => EmbeddingsGenerationConfiguration.GenerateIdentifier(Name);

    public GenAiTransformation GenAiTransformation { get; set; }

    public string Prompt { get; set; }
    
    //TODO: Make this JSON objects? 
    public string JsonSchema { get; set; }
    public string SampleObject { get; set; }
    public string UpdateScript { get; set; }

    public int MaxConcurrency { get; set; } = DefaultMaxConcurrency;


    private List<Transformation> _transforms;

    private const int DefaultMaxConcurrency = 4;

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

    public override bool Validate(out List<string> errors, bool validateName = true, bool validateConnection = true)
    {
        if (validateConnection && Initialized == false)
            throw new InvalidOperationException("GenAi configuration must be initialized");

        errors = [];

        if (validateName && string.IsNullOrEmpty(Name))
            errors.Add($"{nameof(Name)} of GenAi configuration cannot be empty");

        if (TestMode == false && string.IsNullOrEmpty(ConnectionStringName))
            errors.Add($"{nameof(ConnectionStringName)} cannot be empty");

        if (validateConnection && TestMode == false)
        {
            Connection.Validate(errors);
            if (Connection.ModelType != AiModelType.LLM)
                errors.Add($"{nameof(Connection.ModelType)} of GenAI configuration must be {nameof(AiModelType.LLM)}");
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
