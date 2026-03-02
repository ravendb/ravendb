using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.Agents;

/// <summary>
/// Represents a required input parameter used by an AI agent's tools (queries/actions).
/// </summary>
public class AiAgentParameter : IDynamicJson
{
    public AiAgentParameter()
    {
        // for deserialization    
    }

    /// <summary>
    /// Initializes a new parameter with the specified name.
    /// </summary>
    /// <param name="name">The parameter name. Cannot be null or empty.</param>
    public AiAgentParameter(string name)
    {
        ValidationMethods.AssertNotNullOrEmpty(name, nameof(name));
        Name = name;
    }

    /// <summary>
    /// Initializes a new parameter with the specified name and description.
    /// </summary>
    /// <param name="name">The parameter name. Cannot be null or empty.</param>
    /// <param name="description">A human-readable description of the parameter's purpose. Cannot be null or empty.</param>
    public AiAgentParameter(string name, string description) : this(name)
    {
        ValidationMethods.AssertNotNullOrEmpty(description, nameof(description));
        Description = description;
    }

    /// <summary>
    /// Initializes a new agent parameter and controls whether its value should be sent to the LLM.
    /// Use this overload when you need to explicitly hide sensitive values (e.g. userId/tenant/company) from the model.
    /// </summary>
    /// <param name="name">The parameter name. Cannot be null or empty.</param>
    /// <param name="description">
    /// A human-readable description. May be null or empty when using this overload.
    /// </param>
    /// <param name="sendToModel">
    /// When <c>false</c>, the parameter is hidden from the model (it will not be included in prompts/echo messages).
    /// When <c>true</c>, the parameter is exposed to the model.
    /// If you do not call this overload, the default is <see langword="null"/> (treated as exposed).
    /// </param>
    public AiAgentParameter(string name, string description, bool sendToModel) : this(name)
    {
        Description = description;
        SendToModel = sendToModel;
    }

    /// <summary>
    /// Indicates whether the parameter value is exposed to the AI model. 
    /// If <c>null</c>, the parameter is treated as exposed by default.
    /// </summary>
    public bool? SendToModel { get; set; }
    /// <summary>
    /// The parameter name as referenced by tools and scripts.
    /// </summary>
    public string Name { get; set; }
    /// <summary>
    /// Human-readable description explaining what value the parameter expects.
    /// </summary>
    public string Description { get; set; }
    /// <summary>
    /// Serializes this parameter to JSON.
    /// </summary>
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Name)] = Name,
            [nameof(Description)] = Description,
            [nameof(SendToModel)] = SendToModel
        };
    }
}
