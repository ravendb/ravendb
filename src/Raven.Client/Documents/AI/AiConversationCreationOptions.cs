using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.AI;

public class AiConversationCreationOptions : IDynamicJson
{
    // We don't serialize this field, we pass the parameters directly on the body request 
    public Dictionary<string, object> Parameters;

    public AiConversationCreationOptions()
    {
        // for serialization
    }
    public AiConversationCreationOptions(Action<IAiAgentParametersBuilder> builder)
    {
        var aiAgentParameters = new AiAgentParametersBuilder();
        builder?.Invoke(aiAgentParameters);
        Parameters = aiAgentParameters.GetParameters() ?? new Dictionary<string, object>();
    }

    public AiConversationCreationOptions(Dictionary<string, object> parameters)
    {
        Parameters = parameters;
    }

    public int? ConversationExpirationInSec { get; set; }
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ConversationExpirationInSec)] = ConversationExpirationInSec,
            [nameof(Parameters)] = Parameters != null ? DynamicJsonValue.Convert(Parameters) : null,
        };
    }
}
