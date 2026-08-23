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

    public sealed class SystemCollectionsSnapshot
    {
        public SystemCollectionsSnapshot(string databaseId, Dictionary<string, SystemCollectionStats> systemCollections)
        {
            DatabaseId = databaseId;
            SystemCollections = systemCollections;
        }

        public string DatabaseId { get; }

        public Dictionary<string, SystemCollectionStats> SystemCollections { get; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DatabaseId)] = DatabaseId,
                [nameof(SystemCollections)] = DynamicJsonValue.Convert(SystemCollections)
            };
        }
    }

    public sealed class WriteUsageApplicationSnapshot
    {
        public WriteUsageApplicationSnapshot(string applicationName, string topologyId, string changeVector, IReadOnlyList<LastEtagSnapshot> nodes,
            IReadOnlyList<SystemCollectionsSnapshot> systemCollectionsList)
        {
            ApplicationName = applicationName;
            TopologyId = topologyId;
            ChangeVector = changeVector;
            Nodes = nodes;
            SystemCollectionsList = systemCollectionsList;
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
        /// One entry per member of the database group, pairing the member's database id with the last etag
        /// and document count of each of its system ('@'-prefixed) collections. Reported unmerged - the
        /// backend receives every member's own values and aggregates them itself.
        /// </summary>
        public IReadOnlyList<SystemCollectionsSnapshot> SystemCollectionsList { get; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ApplicationName)] = ApplicationName,
                [nameof(TopologyId)] = TopologyId,
                [nameof(ChangeVector)] = ChangeVector,
                [nameof(Nodes)] = new DynamicJsonArray(Nodes.Select(n => n.ToJson())),
                [nameof(SystemCollectionsList)] = new DynamicJsonArray(SystemCollectionsList.Select(s => s.ToJson()))
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
