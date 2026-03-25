using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.AI;

/// <summary>
/// Options used when creating or continuing an AI conversation.
/// Allows passing required parameters for the agent tools and controlling
/// conversation expiration.
/// </summary>
public class AiConversationCreationOptions : IDynamicJson
{
    /// <summary>
    /// Conversation-level parameters passed to the agent.
    /// Each parameter defines a value and whether it should be sent to the model.
    /// </summary>
    public Dictionary<string, AiConversationParameter> Parameters { get; set; }

    /// <summary>
    /// Optional conversation expiration in seconds.
    /// When specified, the server may expire the conversation document after this interval.
    /// </summary>
    public int? ExpirationInSec { get; set; }
    public int? MaxModelIterationsPerCall { get; set; }

    public AiConversationCreationOptions()
    {
    }

    /// <summary>
    /// Adds a named parameter and value to the <see cref="Parameters"/> dictionary.
    /// </summary>
    /// <param name="name">The parameter name.</param>
    /// <param name="value">The parameter value.</param>
    /// <returns>This instance for fluent configuration.</returns>
    public AiConversationCreationOptions AddParameter(string name, object value, AiConversationParameterOptions options = null)
    {
        if (value is not string && value is IEnumerable ie)
            value = new DynamicJsonArray(ie);
        Parameters ??= new Dictionary<string, AiConversationParameter>();
        Parameters.Add(name, new AiConversationParameter { Value = value, SendToModel = options?.SendToModel ?? true});
        return this;
    }

    public AiConversationCreationOptions(Dictionary<string, AiConversationParameter> parameters)
    {
        Parameters = parameters ?? new ();
    }

    public AiConversationCreationOptions(Dictionary<string, object> parameters)
    {
        Parameters = parameters?.ToDictionary(kv => kv.Key, kv => new AiConversationParameter { Value = kv.Value, SendToModel = true}) ?? new ();
    }

    /// <summary>
    /// Serializes the options to a JSON structure.
    /// </summary>
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ExpirationInSec)] = ExpirationInSec,
            [nameof(MaxModelIterationsPerCall)] = MaxModelIterationsPerCall,
            [nameof(Parameters)] = Parameters != null ? DynamicJsonValue.Convert(Parameters) : null,
        };
    }
}

/// <summary>
/// Optional configuration for a conversation parameter.
/// Allows controlling how the parameter is handled when sent to the model.
/// </summary>
public class AiConversationParameterOptions
{
    /// <summary>
    /// Determines whether the parameter should be sent to the model.
    /// 
    /// The parameter will be included in the model input only if:
    /// <list type="bullet">
    /// <item><description>This flag is set to <c>true</c>.</description></item>
    /// <item><description>The parameter is also allowed by the agent configuration.</description></item>
    /// </list>
    /// 
    /// When set to <c>false</c>, the parameter remains available for internal use
    /// (e.g., queries, actions, or sub-agents) but is not exposed to the model.
    /// 
    /// Default is <c>true</c>.
    /// </summary>
    public bool SendToModel { get; set; } = true;
}

/// <summary>
/// Represents a single conversation parameter, including its value
/// and whether it should be sent to the model.
/// </summary>
public class AiConversationParameter : IDynamicJson
{
    /// <summary>
    /// The parameter value.
    /// </summary>
    public object Value { get; set; }

    /// <summary>
    /// Controls whether this parameter is sent to the model at the conversation level.
    /// The parameter will be included only if it is also allowed in the agent configuration
    /// (SendToModel = true or unset there) and this flag is true.
    /// Default is true.
    /// </summary>
    public bool SendToModel { get; set; } = true;
    public DynamicJsonValue ToJson()
    {
        var json = Value as IDynamicJson;
        var val = json == null ? (object)Value : json.ToJson();

        return new DynamicJsonValue
        {
            [nameof(Value)] = val,
            [nameof(SendToModel)] = SendToModel
        };
    }
}
