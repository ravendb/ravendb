using System;
using System.Collections.Generic;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL
{
    public sealed class EtlProcessState : IDatabaseTaskStatus
    {
        public EtlProcessState()
        {
            LastProcessedEtagPerDbId = new Dictionary<string, long>();
            ChangeVector = null;
        }

        public string ConfigurationName { get; set; }

        public string TransformationName { get; set; }

        public Dictionary<string, long> LastProcessedEtagPerDbId { get; set; }

        public string ChangeVector { get; set; }

        public string NodeTag { get; set; }

        // In a regular case we load time-series only when the time-series change.
        // When there is time-series without document (that can happen if the time-series was replicated but its document didn't yet)
        // we mark the time-series of the document as skipped so when we load the document we will load all its time-series with it
        public HashSet<string> SkippedTimeSeriesDocs { get; set; }

        public DateTime? LastBatchTime { get; set; }

        public long GetLastProcessedEtag(string dbId, string nodeTag)
        {
            if (LastProcessedEtagPerDbId.TryGetValue(dbId, out var etag))
                return etag;

            return 0;
        }

        public DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue
            {
                [nameof(ConfigurationName)] = ConfigurationName,
                [nameof(TransformationName)] = TransformationName,
                [nameof(LastProcessedEtagPerDbId)] = LastProcessedEtagPerDbId.ToJson(),
                [nameof(ChangeVector)] = ChangeVector,
                [nameof(NodeTag)] = NodeTag,
                [nameof(SkippedTimeSeriesDocs)] = SkippedTimeSeriesDocs,
                [nameof(LastBatchTime)] = LastBatchTime
            };

            return json;
        }

        public static string GenerateItemName(string databaseName, string configurationName, string transformationName)
        {
            return $"values/{databaseName}/etl/{configurationName.ToLowerInvariant()}/{transformationName.ToLowerInvariant()}";
        }
    }
}
