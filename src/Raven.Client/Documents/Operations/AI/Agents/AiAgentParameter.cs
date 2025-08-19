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
            [nameof(Description)] = Description
        };
    }
}
