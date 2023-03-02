using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions.Stats;
using Raven.Server.Json;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Includes;

public abstract class AbstractIncludesCommand
{
    public static readonly StringSegment IncludesSegment = new(nameof(QueryResult.Includes));
    public static readonly StringSegment CounterIncludesSegment = new(nameof(QueryResult.CounterIncludes));
    public static readonly StringSegment IncludedCounterNamesSegment = new(nameof(QueryResult.IncludedCounterNames));
    public static readonly StringSegment TimeSeriesIncludesSegment = new(nameof(QueryResult.TimeSeriesIncludes));

    private AbstractIncludeDocumentsCommand _includeDocumentsCommand;
    private AbstractIncludeTimeSeriesCommand _includeTimeSeriesCommand;
    private AbstractIncludeCountersCommand _includeCountersCommand;

    protected AbstractIncludesCommand(AbstractIncludeDocumentsCommand includeDocumentsCommand, AbstractIncludeTimeSeriesCommand includeTimeSeriesCommand,
        AbstractIncludeCountersCommand includeCountersCommand)
    {
        _includeDocumentsCommand = includeDocumentsCommand;
        _includeTimeSeriesCommand = includeTimeSeriesCommand;
        _includeCountersCommand = includeCountersCommand;
    }

    protected abstract ValueTask<(long count, long sizeInBytes)> WriteIncludedDocumentsInternalAsync(AsyncBlittableJsonTextWriter writer, JsonOperationContext context,
        CancellationToken token);

    public async ValueTask WriteIncludesAsync(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, SubscriptionBatchStatsScope batchScope, CancellationToken token)
    {
        if (_includeDocumentsCommand is { Count: > 0 })
        {
            writer.WriteStartObject();

            writer.WritePropertyName(context.GetLazyStringForFieldWithCaching(ISubscriptionConnection.TypeSegment));
            writer.WriteValue(BlittableJsonToken.String, context.GetLazyStringForFieldWithCaching(IncludesSegment));
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyStringForFieldWithCaching(IncludesSegment));
            (long count, long sizeInBytes) = await WriteIncludedDocumentsInternalAsync(writer, context, token);
            batchScope?.RecordIncludedDocumentsInfo(count, sizeInBytes);

            writer.WriteEndObject();
        }

        if (_includeCountersCommand is { Count: > 0 })
        {
            writer.WriteStartObject();

            writer.WritePropertyName(context.GetLazyStringForFieldWithCaching(ISubscriptionConnection.TypeSegment));
            writer.WriteValue(BlittableJsonToken.String, context.GetLazyStringForFieldWithCaching(CounterIncludesSegment));
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyStringForFieldWithCaching(CounterIncludesSegment));
            await _includeCountersCommand.WriteIncludesAsync(writer, context, token);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyStringForFieldWithCaching(IncludedCounterNamesSegment));
            writer.WriteIncludedCounterNames(_includeCountersCommand.IncludedCounterNames);
            writer.WriteEndObject();

            batchScope?.RecordIncludedCountersInfo(_includeCountersCommand.GetCountersCount(), _includeCountersCommand.GetCountersSize());
        }

        if (_includeTimeSeriesCommand is { Count: > 0 })
        {
            writer.WriteStartObject();

            writer.WritePropertyName(context.GetLazyStringForFieldWithCaching(ISubscriptionConnection.TypeSegment));
            writer.WriteValue(BlittableJsonToken.String, context.GetLazyStringForFieldWithCaching(TimeSeriesIncludesSegment));
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyStringForFieldWithCaching(TimeSeriesIncludesSegment));
            var size = await _includeTimeSeriesCommand.WriteIncludesAsync(writer, context, token);

            batchScope?.RecordIncludedTimeSeriesInfo(_includeTimeSeriesCommand.GetEntriesCountForStats(), size);

            writer.WriteEndObject();
        }
    }
}
