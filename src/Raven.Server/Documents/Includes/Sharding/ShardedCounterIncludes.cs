using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Includes.Sharding;

public sealed class ShardedCounterIncludes : AbstractIncludeCountersCommand
{
    private readonly CancellationToken _token;
    private Dictionary<string, List<BlittableJsonReaderObject>> _countersByDocumentId;
    private Dictionary<string, HashSet<string>> _includedCounterNames;

    public ShardedCounterIncludes(CancellationToken token = default)
    {
        _token = token;
    }

    public HashSet<string> MissingCounterIncludes { get; set; }

    public override Dictionary<string, string[]> IncludedCounterNames => _includedCounterNames.ToDictionary(x => x.Key, x => x.Value.ToArray());

    public override int Count => _countersByDocumentId?.Count ?? 0;

    public void AddResults(BlittableJsonReaderObject results, Dictionary<string, string[]> includedCounterNames, JsonOperationContext contextToClone)
    {
        if (results == null || results.Count == 0)
            return;

        _countersByDocumentId ??= new Dictionary<string, List<BlittableJsonReaderObject>>(StringComparer.OrdinalIgnoreCase);

        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Normal, "We can get duplicated counter includes when resharding is running. How to deal with that?");

        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();

        for (var i = 0; i < results.Count; i++)
        {
            results.GetPropertyByIndex(i, ref propertyDetails);

            string docId = propertyDetails.Name;

            var countersJsonArray = (BlittableJsonReaderArray)propertyDetails.Value;

            if (_countersByDocumentId.TryGetValue(docId, out var counters) == false)
                _countersByDocumentId[docId] = counters = new List<BlittableJsonReaderObject>();

            if (countersJsonArray.Length > 0)
            {
                for (int j = 0; j < countersJsonArray.Length; j++)
                {
                    var json = countersJsonArray.GetByIndex<BlittableJsonReaderObject>(j);

                    if (json != null)
                        counters.Add(json.Clone(contextToClone));
                    else
                        (MissingCounterIncludes ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase)).Add(docId);
                }
            }
            else
            {
                (MissingCounterIncludes ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase)).Add(docId);
            }
        }

        if (includedCounterNames != null)
        {
            _includedCounterNames ??= new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

            foreach (var kvp in includedCounterNames)
            {
                var docId = kvp.Key;

                if (_includedCounterNames.TryGetValue(docId, out var counterNames) == false)
                    _includedCounterNames[docId] = counterNames = new HashSet<string>();

                foreach (string counterName in kvp.Value)
                {
                    counterNames.Add(counterName);
                }
            }
        }
    }

    public void AddMissingCounter(string docId, BlittableJsonReaderObject counter)
    {
        if (_countersByDocumentId.TryGetValue(docId, out var counters) == false)
            _countersByDocumentId[docId] = counters = new List<BlittableJsonReaderObject>();

        counters.Add(counter);
    }

    public override async ValueTask WriteIncludesAsync(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, CancellationToken token)
    {
        writer.WriteStartObject();

        var first = true;
        foreach (var kvp in _countersByDocumentId)
        {
            if (first == false)
                writer.WriteComma();
            first = false;

            writer.WritePropertyName(kvp.Key);

            writer.WriteStartArray();

            var innerFirst = true;

            foreach (var counter in kvp.Value)
            {
                if (innerFirst == false)
                    writer.WriteComma();
                innerFirst = false;

                if (counter != null)
                    writer.WriteObject(counter);
                else
                    writer.WriteNull();
            }

            await writer.MaybeFlushAsync(token);

            writer.WriteEndArray();
        }

        writer.WriteEndObject();
    }

    public void Gather(List<(BlittableJsonReaderObject Includes, Dictionary<string, string[]> IncludedCounterNames)> list,
        ClusterOperationContext clusterOperationContext)
    {
        foreach (var item in list)
        {
            using (item.Includes)
            {
                _token.ThrowIfCancellationRequested();
                AddResults(item.Includes, item.IncludedCounterNames, clusterOperationContext);
            }
        }
    }

    public override long GetCountersSize()
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Egor, DevelopmentHelper.Severity.Minor, "RavenDB-16279: for task stats in studio should we calculate the stats from each shard or orchestrator");
        return 0L;
    }

    public override long GetCountersCount()
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Egor, DevelopmentHelper.Severity.Minor, "RavenDB-16279: for task stats in studio should we calculate the stats from each shard or orchestrator");
        return 0L;
    }
}
