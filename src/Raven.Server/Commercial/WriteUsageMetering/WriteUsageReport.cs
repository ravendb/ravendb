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

    public sealed class WriteUsageApplicationSnapshot
    {
        public WriteUsageApplicationSnapshot(string applicationName, string topologyId, string changeVector)
        {
            ApplicationName = applicationName;
            TopologyId = topologyId;
            ChangeVector = changeVector;
        }

        public string ApplicationName { get; }

        public string TopologyId { get; }

        public string ChangeVector { get; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ApplicationName)] = ApplicationName,
                [nameof(TopologyId)] = TopologyId,
                [nameof(ChangeVector)] = ChangeVector
            };
        }
    }

    public sealed class WriteUsageSnapshot
    {
        public WriteUsageSnapshot(IReadOnlyList<WriteUsageApplicationSnapshot> databases)
        {
            Databases = databases;
        }

        public IReadOnlyList<WriteUsageApplicationSnapshot> Databases { get; }
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
