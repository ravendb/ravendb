using System;
using Newtonsoft.Json;
using Raven.Client.Documents.Conventions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.AI;

/// <summary>
/// Controls the output format for a single AI conversation turn.
/// <para>
/// A default output schema must still be provided when creating the agent (via <c>CreateAgentAsync</c>).
/// <see cref="AiOutputOptions"/> lets you override that default on a per-call basis — the options set here
/// take precedence over the agent-level schema for the duration of that turn only.
/// </para>
/// </summary>
public class AiOutputOptions
{
    /// <summary>
    /// Override the schema using a sample object. The server converts it to a JSON schema at request time.
    /// Must match the <c>TAnswer</c> type used in the conversation call.
    /// </summary>
    public AiOutputOptions(object sampleObject)
    {
        SampleObject = sampleObject ?? throw new ArgumentNullException(nameof(sampleObject));
    }

    /// <summary>
    /// Override the schema using an explicit JSON schema string.
    /// </summary>
    public AiOutputOptions(string outputSchema)
    {
        if (string.IsNullOrWhiteSpace(outputSchema))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(outputSchema));
        OutputSchema = outputSchema;
    }

    /// <summary>
    /// Creates empty options. Set <see cref="NoSchema"/> to <c>true</c> to disable structured output for the turn.
    /// </summary>
    public AiOutputOptions()
    {
    }

    /// <summary>
    /// A sample object used to generate a JSON schema for structured output.
    /// </summary>
    public object SampleObject { get; internal set; }

    /// <summary>
    /// An explicit JSON schema string for structured output.
    /// Takes precedence over <see cref="SampleObject"/> if both are set.
    /// </summary>
    public string OutputSchema { get; internal set; }

    /// <summary>
    /// When true, disables structured output entirely.
    /// The LLM returns free-form text instead of JSON conforming to a schema.
    /// </summary>
    public bool NoSchema { get; set; }

    public DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
    {
        var json = new DynamicJsonValue();
        if (SampleObject != null)
            json[nameof(SampleObject)] = conventions.Serialization.DefaultConverter.ToBlittable(SampleObject, context).ToString();
        if (OutputSchema != null)
            json[nameof(OutputSchema)] = OutputSchema;
        if (NoSchema)
            json[nameof(NoSchema)] = true;
        return json;
    }
}
