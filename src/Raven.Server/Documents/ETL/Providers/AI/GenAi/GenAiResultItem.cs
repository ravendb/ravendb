using System.Collections.Generic;
using Raven.Client.Documents.Operations.AI;
using System.Linq;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.AI.GenAi;

public class GenAiResultItem
{
    public ModelOutput ModelOutput { get; set; }

    public ContextOutput ContextOutput { get; set; }

    public string DocumentId { get; set; }

    internal bool UpdateHash { get; set; } = true;

    internal string ConversationDocumentId { get; set;}

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ContextOutput)] = ContextOutput?.ToJson(),
            [nameof(ModelOutput)] = ModelOutput?.ToJson(),
            [nameof(DocumentId)] = DocumentId
        };
    }
}

public class ModelOutput
{
    public BlittableJsonReaderObject Output { get; set; }
    public AiUsage Usage { get; set; }
    public BlittableJsonReaderObject ConversationDocument { get; set; }

    public DynamicJsonValue ToJson() => new()
    {
        [nameof(Usage)] = Usage?.ToJson(),
        [nameof(Output)] = Output,
        [nameof(ConversationDocument)] = ConversationDocument
    };
}

public class ContextOutput
{
    public BlittableJsonReaderObject Context { get; set; }
    
    public List<AiAttachment> Attachments;
    public bool IsCached { get; set; }
    public string AiHash { get; set; }

    public DynamicJsonValue ToJson()
    {
        var json = new DynamicJsonValue
        {
            [nameof(Context)] = Context, 
            [nameof(IsCached)] = IsCached, 
            [nameof(AiHash)] = AiHash
        };
        if (Attachments != null)
            json[nameof(Attachments)] = new DynamicJsonArray(Attachments.Select(x => x.ToJson()));

        return json;
    }
}
