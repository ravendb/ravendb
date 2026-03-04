using System;
using System.Collections;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.AI;

public class AiConversationCreationOptions : IDynamicJson
{
    public Dictionary<string, object> Parameters { get; set; }
    public int? ExpirationInSec { get; set; }
    public int? MaxModelIterationsPerCall { get; set; }

    public AiConversationCreationOptions()
    {
         
    }
    public AiConversationCreationOptions AddParameter(string name, object value)
    {
        if (value is not string && value is IEnumerable ie)
            value = new DynamicJsonArray(ie);
        Parameters ??= new Dictionary<string, object>();
        Parameters.Add(name, value);
        return this;
    }

    public AiConversationCreationOptions(Dictionary<string, object> parameters)
    {
        Parameters = parameters;
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
