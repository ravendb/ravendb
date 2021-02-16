using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.ETL
{
    public class UpdateEtlProcessStateCommand : UpdateValueForDatabaseCommand
    {
        public string ConfigurationName { get; set; }

        public string TransformationName { get; set; }

        public long LastProcessedEtag { get; set; }

        public string ChangeVector { get; set; }

        public string NodeTag { get; set; }

        public bool HasHighlyAvailableTasks;

        public HashSet<string> SkippedTimeSeriesDocs { get; set; }

        public long LastBatchTime { get; set; }

        private UpdateEtlProcessStateCommand()
        {
            // for deserialization
        }

        public UpdateEtlProcessStateCommand(string databaseName, string configurationName, string transformationName, long lastProcessedEtag, string changeVector,
            string nodeTag, bool hasHighlyAvailableTasks, string uniqueRequestId, HashSet<string> skippedTimeSeriesDocs, long lastBatchTimeMilliseconds) : base(databaseName, uniqueRequestId)
        {
            ConfigurationName = configurationName;
            TransformationName = transformationName;
            LastProcessedEtag = lastProcessedEtag;
            ChangeVector = changeVector;
            NodeTag = nodeTag;
            HasHighlyAvailableTasks = hasHighlyAvailableTasks;
            SkippedTimeSeriesDocs = skippedTimeSeriesDocs;

            if (lastBatchTimeMilliseconds > 0)
                LastBatchTime = lastBatchTimeMilliseconds;
        }

        public override string GetItemId()
        {
            return EtlProcessState.GenerateItemName(DatabaseName, ConfigurationName, TransformationName);
        }

        private IDatabaseTask GetMatchingConfiguration(RawDatabaseRecord record)
        {
            var ravenEtls = record.RavenEtls;
            if (ravenEtls != null)
            {
                for (var i = 0; i < ravenEtls.Count; i++)
                {
                    if (ravenEtls[i].Name == ConfigurationName)
                    {
                        return ravenEtls[i];
                    }
                }
            }

            var sqlEtls = record.SqlEtls;
            if (sqlEtls != null)
            {
                for (var i = 0; i < sqlEtls.Count; i++)
                {
                    if (sqlEtls[i].Name == ConfigurationName)
                    {
                        return sqlEtls[i];
                    }
                }
            }

            var parquetEtls = record.OlapEtls;
            if (parquetEtls != null)
            {
                for (var i = 0; i < parquetEtls.Count; i++)
                {
                    if (parquetEtls[i].Name == ConfigurationName)
                    {
                        return parquetEtls[i];
                    }
                }
            }

            return null;
        }

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, RawDatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue)
        {
            EtlProcessState etlState;

            if (existingValue != null)
            {
                etlState = JsonDeserializationClient.EtlProcessState(existingValue);

                var databaseTask = GetMatchingConfiguration(record);

                if (databaseTask == null)
                    throw new RachisApplyException($"Can't update progress of ETL {ConfigurationName} by node {NodeTag}, because it's configuration can't be found");

                var topology = record.Topology;
                var lastResponsibleNode = GetLastResponsibleNode(HasHighlyAvailableTasks, topology, NodeTag);
                if (topology.WhoseTaskIsIt(RachisState.Follower, databaseTask, lastResponsibleNode) != NodeTag)
                    throw new RachisApplyException($"Can't update progress of ETL {ConfigurationName} by node {NodeTag}, because it's not its task to update this ETL");
            }
            else
            {
                etlState = new EtlProcessState
                {
                    ConfigurationName = ConfigurationName,
                    TransformationName = TransformationName
                };
            }

            etlState.LastProcessedEtagPerNode[NodeTag] = LastProcessedEtag;
            etlState.ChangeVector = ChangeVector;
            etlState.NodeTag = NodeTag;
            etlState.SkippedTimeSeriesDocs = SkippedTimeSeriesDocs;
            etlState.LastBatchTime = LastBatchTime;


            return context.ReadObject(etlState.ToJson(), GetItemId());
        }

        public static Func<string> GetLastResponsibleNode(
           bool hasHighlyAvailableTasks,
           DatabaseTopology topology,
           string nodeTag)
        {
            return () =>
            {
                if (hasHighlyAvailableTasks)
                    return null;

                if (topology.Members.Contains(nodeTag) == false)
                    return null;

                return nodeTag;
            };
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(ConfigurationName)] = ConfigurationName;
            json[nameof(TransformationName)] = TransformationName;
            json[nameof(LastProcessedEtag)] = LastProcessedEtag;
            json[nameof(ChangeVector)] = ChangeVector;
            json[nameof(NodeTag)] = NodeTag;
            json[nameof(HasHighlyAvailableTasks)] = HasHighlyAvailableTasks;
            json[nameof(SkippedTimeSeriesDocs)] = SkippedTimeSeriesDocs;
        }
    }
}
