using System.Collections.Generic;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.AI.GenAi;

public record GenAiAttachment(string Name, string Type, string Data);

public record GenAiScriptResult(string DocumentId, BlittableJsonReaderObject Context, string AiHash, bool IsCached)
{
    public List<GenAiAttachment> Attachments;
}
