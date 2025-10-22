using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace Raven.Server.Documents.AI.AiAssistant.Requests;

public abstract class AiAssistRequestBase
{
    [JsonConverter(typeof(StringEnumConverter))]
    public AiAssistantOperationType OperationType { get; set; }
}
