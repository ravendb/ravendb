using System.Collections.Generic;
using Raven.Server.Documents.ETL.Providers.AI;
using Sparrow.Json;

namespace Raven.Server.Documents.AI;

// A provider-independent chat-completion request; the selected provider translates it into its own wire format.
public sealed record AiChatRequest
{
    public IEnumerable<BlittableJsonReaderObject> Messages;

    public List<AiAttachment> Attachments;

    // Provider-shaped tools, prepared once per conversation call (see ChatCompletionClient.PrepareTools).
    public List<BlittableJsonReaderObject> PreparedTools;

    public bool UseTools;

    public string Schema;

    public string PromptCacheKey;
}

public sealed class AiToolDescriptor
{
    public string Name;
    public string Description;
    public string ParametersSchema;

    public AiToolDescriptor(string name, string description, string parametersSchema)
    {
        Name = name;
        Description = description;
        ParametersSchema = parametersSchema;
    }
}
