using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Includes.Sharding;

public sealed class ShardedTimeSeriesIncludes : AbstractIncludeTimeSeriesCommand
{
    private readonly bool _supportsMissingIncludes;
    private readonly CancellationToken _token;
    public ShardedTimeSeriesIncludes(bool supportsMissingIncludes, CancellationToken token = default)
    {
        _supportsMissingIncludes = supportsMissingIncludes;
        _token = token;
    }

    private Dictionary<string, BlittableJsonReaderObject> _resultsByDocumentId;

    public override int Count => _resultsByDocumentId?.Count ?? 0;

    public Dictionary<string, List<TimeSeriesRange>> MissingTimeSeriesIncludes { get; set; }

    public void AddResults(BlittableJsonReaderObject results, JsonOperationContext contextToClone)
    {
        if (results == null || results.Count == 0)
            return;

        _resultsByDocumentId ??= new(StringComparer.OrdinalIgnoreCase);

        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
        for (var i = 0; i < results.Count; i++)
        {
            results.GetPropertyByIndex(i, ref propertyDetails);

            string documentId = propertyDetails.Name;

            var json = (BlittableJsonReaderObject)propertyDetails.Value;

            if (_supportsMissingIncludes == false || HasMissingEntries(json, out var missingRanges) == false)
            {
                var added = _resultsByDocumentId.TryAdd(documentId, json.Clone(contextToClone));

                if (added == false)
                {
                    DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Normal, "We can get duplicated TS includes when resharding is running. How to deal with that?");
                    throw new NotImplementedException("Handling of duplicated TS includes during resharding");
                }
            }
            else
            {
                (MissingTimeSeriesIncludes ??= new Dictionary<string, List<TimeSeriesRange>>(StringComparer.OrdinalIgnoreCase)).TryAdd(documentId, missingRanges);
            }
            
        }
    }

    private bool HasMissingEntries(BlittableJsonReaderObject json, out List<TimeSeriesRange> missingRanges)
    {
        missingRanges = null;

        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();

        var missingEntries = false;

        for (int i = 0; i < json.Count; i++)
        {
            json.GetPropertyByIndex(i, ref propertyDetails);

            var timeSeriesJsonArray = (BlittableJsonReaderArray)propertyDetails.Value;

            if (timeSeriesJsonArray.Length > 0)
            {
                for (int j = 0; j < timeSeriesJsonArray.Length; j++)
                {
                    var tsJson = timeSeriesJsonArray.GetByIndex<BlittableJsonReaderObject>(j);

                    if (tsJson.TryGet<BlittableJsonReaderArray>(nameof(TimeSeriesRangeResult.Entries), out var entries))
                    {
                        if (entries.Length == 0)
                        {
                            missingEntries = true;

                            tsJson.TryGet<DateTime?>(nameof(TimeSeriesRangeResult.From), out var from);
                            tsJson.TryGet<DateTime?>(nameof(TimeSeriesRangeResult.To), out var to);

                            (missingRanges ??= new List<TimeSeriesRange>()).Add(new TimeSeriesRange()
                            {
                                Name = propertyDetails.Name,
                                From = from,
                                To = to
                            });
                        }
                    }
                }
            }
        }

        return missingEntries;
    }

    public void AddMissingTimeSeries(string docId, BlittableJsonReaderObject timeSeries)
    {
        _resultsByDocumentId.TryAdd(docId, timeSeries);
    }

    public override async ValueTask<int> WriteIncludesAsync(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, CancellationToken token)
    {
        int size = 0;
        writer.WriteStartObject();

        var first = true;
        foreach (var kvp in _resultsByDocumentId)
        {
            if (first == false)
                writer.WriteComma();

            first = false;

            writer.WritePropertyName(kvp.Key);
            writer.WriteObject(kvp.Value);

            size += kvp.Key.Length;
            size += kvp.Value.Size;

            await writer.MaybeFlushAsync(token);
        }

        writer.WriteEndObject();

        return size;
    }

    public override long GetEntriesCountForStats()
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Egor, DevelopmentHelper.Severity.Minor, "RavenDB-16279: for task stats in studio should we calculate the stats from each shard or orchestrator");
        return 0L;
    }

    public void Gather(List<BlittableJsonReaderObject> list, ClusterOperationContext clusterOperationContext)
    {
        foreach (var item in list)
        {
            using (item)
            {
                _token.ThrowIfCancellationRequested();
                AddResults(item, clusterOperationContext);
            }
        }
    }
}
