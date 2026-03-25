using System;
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
    /// Initializes a new agent parameter with the specified name.
    /// </summary>
    /// <param name="name">The parameter name. Cannot be null or empty.</param>
    public AiAgentParameter(string name)
    {
        ValidationMethods.AssertNotNullOrEmpty(name, nameof(name));
        Name = name;
    }

    /// <summary>
    /// Initializes a new agent parameter with a name and description.
    /// </summary>
    /// <inheritdoc cref="AiAgentParameter(string)" />
    /// <param name="description">
    /// A human-readable description. May be null or empty when using this overload.
    /// </param>
    public AiAgentParameter(string name, string description) : this(name)
    {
        ValidationMethods.AssertNotNullOrEmpty(description, nameof(description));
        Description = description;
    }

    /// <summary>
    /// Initializes a new agent parameter and controls whether its value should be sent to the LLM.
    /// Use this overload when you need to explicitly hide sensitive values (e.g. email / SSN / credit card number) from the model.
    /// </summary>
    /// <inheritdoc cref="AiAgentParameter(string, string)" />
    /// <param name="sendToModel">
    /// When <c>false</c>, the parameter is hidden from the model (it will not be included in prompts/echo messages).
    /// When <c>true</c>, the parameter is exposed to the model.
    /// If you do not call this overload, the default is <see langword="null"/> (treated as exposed).
    /// </param>
    public AiAgentParameter(string name, string description, bool sendToModel) : this(name, description)
    {
        SendToModel = sendToModel;
    }

    /// <summary>
    /// Initializes a new agent parameter and controls whether its value should be sent to the LLM.
    /// Use this overload when you need to explicitly hide sensitive values (e.g. email / SSN / credit card number) from the model.
    /// </summary>
    /// <inheritdoc cref="AiAgentParameter(string, string, bool)" />
    /// <param name="policy">
    /// Policy flags for this parameter.
    /// Use <see cref="AiAgentParameterPolicy.ForbidModelGeneration"/> to prevent
    /// the parent agent from generating a value for this parameter when the agent
    /// is used as a sub-agent.
    /// The value may only be inherited from the parent agent, if a parameter with
    /// the same name exists.
    /// </param>
    public AiAgentParameter(string name, string description, bool sendToModel, AiAgentParameterPolicy policy) : this(name, description, sendToModel)
    {
        Policy = policy;
    }

    /// <summary>
    /// Initializes a new agent parameter with a name, description, and policy flags.
    /// </summary>
    /// <inheritdoc cref="AiAgentParameter(string, string)" />
    /// <param name="policy">
    /// Policy flags for this parameter.
    /// When <see cref="AiAgentParameterPolicy.ForbidModelGeneration"/> is set
    /// and this agent is used as a sub-agent, the parent agent cannot generate
    /// a value for this parameter.
    /// The value may only be inherited from the parent agent's parameters,
    /// ensuring it is not model-generated.
    /// </param>
    public AiAgentParameter(string name, string description, AiAgentParameterPolicy policy) : this(name, description)
    {
        Policy = policy;
    }

    /// <summary>
    /// Initializes a new agent parameter and controls whether its value should be sent to the LLM.
    /// Use this overload when you need to explicitly hide sensitive values (e.g. email / SSN / credit card number) from the model.
    /// </summary>
    /// <inheritdoc cref="AiAgentParameter(string, string, bool, AiAgentParameterPolicy)" />
    /// <param name="type">
    /// Specifies the expected <see cref="AiAgentParameterValueType"/> for this parameter.
    /// When set to a concrete value, the agent validates the provided value against it;
    /// <see cref="AiAgentParameterValueType.Default"/> disables type validation (backward compatibility).
    /// </param>
    public AiAgentParameter(string name, string description, bool sendToModel, AiAgentParameterPolicy policy, AiAgentParameterValueType type) : this(name, description, sendToModel, policy)
    {
        Type = type;
    }

    /// <summary>
    /// Defines whether the parameter is included in the data sent to the model
    /// </summary>
    public bool? SendToModel { get; set; }
    /// <summary>
    /// The parameter name as referenced by tools and scripts.
    /// </summary>
    public string Name { get; set; }

    /// <summary>
    /// A human-readable description of the parameter.
    /// </summary>
    public string Description { get; set; }

    /// <summary>
    /// Defines how this parameter is handled:
    /// how it behaves when a sub-agent defines a parameter
    /// with the same <see cref="Name"/>.
    /// </summary>
    /// <remarks>
    /// When <see cref="AiAgentParameterPolicy.ForbidModelGeneration"/> is set and this agent is used
    /// as a sub-agent, the parent agent isn't allowed to generate a parameter value for the sub-agent;
    /// the parameter's value may only be inherited from the parent agent parameters.
    /// This ensures that this is a trusted value.
    /// </remarks>
    public AiAgentParameterPolicy Policy { get; set; } = AiAgentParameterPolicy.Default;

    /// <summary>
    /// Specifies the expected JSON value type for this parameter.
    /// </summary>
    /// <remarks>
    /// When set to a specific <see cref="AiAgentParameterValueType"/>, the agent validates that
    /// the provided value matches the declared type before execution.
    /// 
    /// If set to <see cref="AiAgentParameterValueType.Default"/>, no type validation is performed
    /// (for backward compatibility with existing agents).
    /// 
    /// For array types (e.g. <see cref="AiAgentParameterValueType.ArrayOfString"/>),
    /// all items in the array must be of the declared element type.
    /// </remarks>
    public AiAgentParameterValueType Type { get; set; } = AiAgentParameterValueType.Default;

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Name)] = Name,
            [nameof(Description)] = Description,
            [nameof(SendToModel)] = SendToModel,
            [nameof(Policy)] = Policy,
            [nameof(Type)] = Type,
        };
    }
}

/// <summary>
/// Defines the expected JSON value type of an agent parameter.
/// Used for validation before execution.
/// <see cref="AiAgentParameterValueType.Default"/> disables type validation.
/// </summary>
public enum AiAgentParameterValueType
{
    Default, // Don't care - for backward compatibility
    String,
    Number,
    Boolean,
    ArrayOfString,
    ArrayOfNumber,
    ArrayOfBoolean,
    Null
}

/// <summary>
/// Defines policy flags that control how a parameter behaves,
/// especially when used across parent and sub-agent boundaries.
/// </summary>
[Flags]
public enum AiAgentParameterPolicy
{
    /// <summary>
    /// No special behavior.
    /// </summary>
    Default = 0,

    /// <summary>
    /// Prevents the model from generating a value for this parameter.
    /// When used in a sub-agent, the value may only be inherited
    /// from the parent agent's parameters.
    /// </summary>
    ForbidModelGeneration = 1
}
