using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial.WriteUsageMetering
{
    public static class WriteUsageMeteringConstants
    {
        // Single api.ravendb.net endpoint for Quill usage: PUT to report write-usage, POST to query it.
        public const string UsageEndpointPath = "/api/v1/quill/usage";
    }

    public sealed class WriteUsageNodeSnapshot
    {
        public WriteUsageNodeSnapshot(string databaseId, long lastEtag, Dictionary<string, long> systemCollections)
        {
            DatabaseId = databaseId;
            LastEtag = lastEtag;
            SystemCollections = systemCollections;
        }

        public string DatabaseId { get; }

        public long LastEtag { get; }

        public Dictionary<string, long> SystemCollections { get; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DatabaseId)] = DatabaseId,
                [nameof(LastEtag)] = LastEtag,
                [nameof(SystemCollections)] = DynamicJsonValue.Convert(SystemCollections)
            };
        }
    }

    public sealed class WriteUsageApplicationSnapshot
    {
        public WriteUsageApplicationSnapshot(string applicationName, string topologyId, string changeVector, IReadOnlyList<WriteUsageNodeSnapshot> nodes)
        {
            ApplicationName = applicationName;
            TopologyId = topologyId;
            ChangeVector = changeVector;
            Nodes = nodes;
        }

        public string ApplicationName { get; }

        public string TopologyId { get; }

        public string ChangeVector { get; }

        public IReadOnlyList<WriteUsageNodeSnapshot> Nodes { get; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ApplicationName)] = ApplicationName,
                [nameof(TopologyId)] = TopologyId,
                [nameof(ChangeVector)] = ChangeVector,
                [nameof(Nodes)] = new DynamicJsonArray(Nodes.Select(n => n.ToJson()))
            };
        }
    }

    public sealed class WriteUsageSnapshot
    {
        public WriteUsageSnapshot(IReadOnlyList<WriteUsageApplicationSnapshot> applications)
        {
            Applications = applications;
        }

        public IReadOnlyList<WriteUsageApplicationSnapshot> Applications { get; }
    }


    public sealed class WriteUsageReport
    {
        public WriteUsageReport(DynamicJsonValue license, IReadOnlyList<WriteUsageApplicationSnapshot> applications)
        {
            License = license;
            Applications = applications;
        }

        public DynamicJsonValue License { get; }

        public IReadOnlyList<WriteUsageApplicationSnapshot> Applications { get; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(License)] = License,
                [nameof(Applications)] = new DynamicJsonArray(Applications.Select(d => d.ToJson()))
            };
        }
    }
}
