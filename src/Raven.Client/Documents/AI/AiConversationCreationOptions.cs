using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.AI;

public class AiConversationCreationOptions : IDynamicJson
{
    public Dictionary<string, AiConversationParameter> Parameters { get; set; }
    public int? ExpirationInSec { get; set; }
    public int? MaxModelIterationsPerCall { get; set; }

    public AiConversationCreationOptions()
    {
    }

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
        Parameters = parameters;
    }

    public AiConversationCreationOptions(Dictionary<string, object> parameters)
    {
        Parameters = parameters.ToDictionary(kv => kv.Key, kv => new AiConversationParameter { Value = kv.Value, SendToModel = true});
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ExpirationInSec)] = ExpirationInSec,
            [nameof(Parameters)] = Parameters != null ? DynamicJsonValue.Convert(Parameters) : null,
        };
    }
}

public class AiConversationParameterOptions
{
    public bool SendToModel { get; set; } = true;
}

public class AiConversationParameter : IDynamicJson
{
    public object Value { get; set; }
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
