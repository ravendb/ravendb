using System;
using System.Collections.Generic;
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
    /// A dictionary of named parameters required by the agent's tools (queries/actions)
    /// and used to initialize the conversation context.
    /// </summary>
    public Dictionary<string, object> Parameters { get; set; }

    /// <summary>
    /// Optional conversation expiration in seconds.
    /// When specified, the server may expire the conversation document after this interval.
    /// </summary>
    public int? ExpirationInSec { get; set; }

    public AiConversationCreationOptions()
    {
         
    }

    /// <summary>
    /// Adds a named parameter and value to the <see cref="Parameters"/> dictionary.
    /// </summary>
    /// <param name="name">The parameter name.</param>
    /// <param name="value">The parameter value.</param>
    /// <returns>This instance for fluent configuration.</returns>
    public AiConversationCreationOptions AddParameter(string name, object value)
    {
        Parameters ??= new Dictionary<string, object>();
        Parameters.Add(name, value);
        return this;
    }

    /// <summary>
    /// Initializes a new instance with the specified parameters.
    /// </summary>
    /// <param name="parameters">The initial parameters to use for the conversation.</param>
    public AiConversationCreationOptions(Dictionary<string, object> parameters)
    {
        Parameters = parameters;
    }

    /// <summary>
    /// Serializes the options to a JSON structure.
    /// </summary>
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ExpirationInSec)] = ExpirationInSec,
            [nameof(Parameters)] = Parameters != null ? DynamicJsonValue.Convert(Parameters) : null,
        };
    }
}
