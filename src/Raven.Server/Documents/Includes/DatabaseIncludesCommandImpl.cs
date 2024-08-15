using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Includes;

public class DatabaseIncludesCommandImpl : AbstractIncludesCommand
{
    public readonly IncludeDocumentsCommand IncludeDocumentsCommand;
    public readonly IncludeTimeSeriesCommand IncludeTimeSeriesCommand;
    public readonly IncludeCountersCommand IncludeCountersCommand;

    public DatabaseIncludesCommandImpl(IncludeDocumentsCommand includeDocumentsCommand, IncludeTimeSeriesCommand includeTimeSeriesCommand, IncludeCountersCommand includeCountersCommand) 
        : base(includeDocumentsCommand, includeTimeSeriesCommand, includeCountersCommand)
    {
        IncludeDocumentsCommand = includeDocumentsCommand;
        IncludeTimeSeriesCommand = includeTimeSeriesCommand;
        IncludeCountersCommand = includeCountersCommand;
    }

    public override ValueTask<(long count, long sizeInBytes)> WriteIncludedDocumentsInternalAsync(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, CancellationToken token)
    {
        var includes = new List<Document>();
        IncludeDocumentsCommand.Fill(includes, includeMissingAsNull: true);
        return writer.WriteIncludesAsync(context, includes, token);
    }

    public void GatherIncludesForDocument(Document document)
    {
        IncludeDocumentsCommand?.Gather(document);
        IncludeTimeSeriesCommand?.Fill(document);
        IncludeCountersCommand?.Fill(document);
    }
}
