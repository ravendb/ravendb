using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.Agents;

public class AiAgentParameter : IDynamicJson
{
    public AiAgentParameter()
    {
        // for deserialization    
    }

    public AiAgentParameter(string name)
    {
        ValidationMethods.AssertNotNullOrEmpty(name, nameof(name));
        Name = name;
    }

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

    public bool? SendToModel { get; set; }
    public string Name { get; set; }
    public string Description { get; set; }
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
