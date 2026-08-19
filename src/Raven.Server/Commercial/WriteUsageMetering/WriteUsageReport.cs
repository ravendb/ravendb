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

    public sealed class LastEtagSnapshot
    {
        public LastEtagSnapshot(string databaseId, long lastEtag)
        {
            DatabaseId = databaseId;
            LastEtag = lastEtag;
        }

        public string DatabaseId { get; }

        public long LastEtag { get; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DatabaseId)] = DatabaseId,
                [nameof(LastEtag)] = LastEtag
            };
        }
    }

    public sealed class SystemCollectionStats : IDynamicJson
    {
        public long Etag;
        public long Count;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Etag)] = Etag,
                [nameof(Count)] = Count
            };
        }
    }

    public sealed class WriteUsageApplicationSnapshot
    {
        public WriteUsageApplicationSnapshot(string applicationName, string topologyId, string changeVector, IReadOnlyList<LastEtagSnapshot> nodes, Dictionary<string, SystemCollectionStats> systemCollections)
        {
            ApplicationName = applicationName;
            TopologyId = topologyId;
            ChangeVector = changeVector;
            Nodes = nodes;
            SystemCollections = systemCollections;
        }

        public string ApplicationName { get; }

        public string TopologyId { get; }

        public string ChangeVector { get; }

        /// <summary>
        /// One entry per member of the database group. Members that haven't reported yet, or that report
        /// without a database id (unloaded, faulted, or a sharded orchestrator), are not included.
        /// </summary>
        public IReadOnlyList<LastEtagSnapshot> Nodes { get; }

        /// <summary>
        /// Last etag and document count per system ('@'-prefixed) collection, merged over the members
        /// of the database group.
        /// </summary>
        public Dictionary<string, SystemCollectionStats> SystemCollections { get; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ApplicationName)] = ApplicationName,
                [nameof(TopologyId)] = TopologyId,
                [nameof(ChangeVector)] = ChangeVector,
                [nameof(Nodes)] = new DynamicJsonArray(Nodes.Select(n => n.ToJson())),
                [nameof(SystemCollections)] = DynamicJsonValue.Convert(SystemCollections)
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
