using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Includes.Sharding;

public sealed class ShardIncludesCommandImpl : DatabaseIncludesCommandImpl
{
    public ShardIncludesCommandImpl(IncludeDocumentsCommand includeDocumentsCommand, IncludeTimeSeriesCommand includeTimeSeriesCommand,
        IncludeCountersCommand includeCountersCommand)
        : base(includeDocumentsCommand, includeTimeSeriesCommand, includeCountersCommand)
    {

    }

    protected override ValueTask<(long count, long sizeInBytes)> WriteIncludedDocumentsInternalAsync(AsyncBlittableJsonTextWriter writer, JsonOperationContext context,
        CancellationToken token)
    {
        var includes = new List<Document>();
        IncludeDocumentsCommand.Fill(includes, includeMissingAsNull: true);
        return writer.WriteIncludesAsync(context, includes, token);
    }
}
