﻿using System.Collections.Generic;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Extensions;
using Sparrow.Json.Parsing;

namespace Raven.Client.Server.ETL
{
    public class EtlProcessState
    {
        public EtlProcessState()
        {
            LastProcessedEtagPerNode = new Dictionary<string, long>();
            ChangeVector = new ChangeVectorEntry[0];
        }

        public string Destination { get; set; }

        public string TransformationName { get; set; }

        public Dictionary<string, long> LastProcessedEtagPerNode { get; set; }

        public ChangeVectorEntry[] ChangeVector { get; set; }

        public long GetLastProcessedEtagForNode(string nodeTag)
        {
            if (LastProcessedEtagPerNode.TryGetValue(nodeTag, out var etag))
                return etag;

            return 0;
        }

        public DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue
            {
                [nameof(Destination)] = Destination,
                [nameof(TransformationName)] = TransformationName,
                [nameof(LastProcessedEtagPerNode)] = LastProcessedEtagPerNode.ToJson(),
                [nameof(ChangeVector)] = ChangeVector.ToJson(),
            };

            return json;
        }

        public static string GenerateItemName(string databaseName, string destinationName, string transformationName)
        {
            return $"values/{databaseName}/etl/{destinationName.ToLowerInvariant()}/{transformationName.ToLowerInvariant()}";
        }
    }
}