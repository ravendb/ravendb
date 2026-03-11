using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.AI;

public class AiConversationCreationOptions : IDynamicJson
{
    public Dictionary<string, AiConversationParameterValue> Parameters { get; set; }
    public int? ExpirationInSec { get; set; }
    public int? MaxModelIterationsPerCall { get; set; }

    public AiConversationCreationOptions()
    {
    }

    public AiConversationCreationOptions AddParameter(string name, object value, bool sendToModel = true)
    {
        if (value is not string && value is IEnumerable ie)
            value = new DynamicJsonArray(ie);
        Parameters ??= new Dictionary<string, AiConversationParameterValue>();
        Parameters.Add(name, new AiConversationParameterValue { Value = value, SendToModel = sendToModel});
        return this;
    }

    public AiConversationCreationOptions(Dictionary<string, AiConversationParameterValue> parameters)
    {
        Parameters = parameters;
    }

    public AiConversationCreationOptions(Dictionary<string, object> parameters)
    {
        Parameters = parameters.ToDictionary(kv => kv.Key, kv => new AiConversationParameterValue { Value = kv.Value, SendToModel = true});
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

public class AiConversationParameterValue : IDynamicJson
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
