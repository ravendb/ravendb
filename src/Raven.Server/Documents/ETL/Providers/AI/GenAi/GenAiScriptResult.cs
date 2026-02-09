using System.Collections.Generic;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.AI.GenAi;

public record GenAiScriptResult(string DocumentId, BlittableJsonReaderObject Context, string AiHash, bool IsCached, bool IsMetadataCleanupMarker = false)
{
    public List<AiAttachment> Attachments;
}
