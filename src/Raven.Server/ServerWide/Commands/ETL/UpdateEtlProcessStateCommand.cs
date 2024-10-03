using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.ETL
{
    public sealed class UpdateEtlProcessStateCommand : UpdateValueForDatabaseCommand
    {
        public string ConfigurationName { get; set; }

        public string TransformationName { get; set; }

        public long LastProcessedEtag { get; set; }

        public string ChangeVector { get; set; }

        public string NodeTag { get; set; }

        public string DbId { get; set; }

        public bool HasHighlyAvailableTasks;

        public HashSet<string> SkippedTimeSeriesDocs { get; set; }

        public DateTime? LastBatchTime { get; set; }

        private UpdateEtlProcessStateCommand()
        {
            // for deserialization
        }

        public UpdateEtlProcessStateCommand(string databaseName, string configurationName, string transformationName, long lastProcessedEtag, string changeVector,
            string nodeTag, bool hasHighlyAvailableTasks, string dbId, string uniqueRequestId, HashSet<string> skippedTimeSeriesDocs, DateTime? lastBatchTime) : base(databaseName, uniqueRequestId)
        {
            ConfigurationName = configurationName;
            TransformationName = transformationName;
            LastProcessedEtag = lastProcessedEtag;
            ChangeVector = changeVector;
            NodeTag = nodeTag;
            HasHighlyAvailableTasks = hasHighlyAvailableTasks;
            DbId = dbId;
            SkippedTimeSeriesDocs = skippedTimeSeriesDocs;

            if (lastBatchTime.HasValue)
                LastBatchTime = lastBatchTime;
        }

        public override string GetItemId()
        {
            var databaseName = ShardHelper.ToDatabaseName(DatabaseName);

            return EtlProcessState.GenerateItemName(databaseName, ConfigurationName, TransformationName);
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

            var olapEtls = record.OlapEtls;
            if (olapEtls != null)
            {
                for (var i = 0; i < olapEtls.Count; i++)
                {
                    if (olapEtls[i].Name == ConfigurationName)
                    {
                        return olapEtls[i];
                    }
                }
            }

            var elasticEtls = record.ElasticSearchEtls;
            if (elasticEtls != null)
            {
                for (var i = 0; i < elasticEtls.Count; i++)
                {
                    if (elasticEtls[i].Name == ConfigurationName)
                    {
                        return elasticEtls[i];
                    }
                }
            }

            var queueEtls = record.QueueEtls;
            if (queueEtls != null)
            {
                for (var i = 0; i < queueEtls.Count; i++)
                {
                    if (queueEtls[i].Name == ConfigurationName)
                    {
                        return queueEtls[i];
                    }
                }
            }
            
            var snowflakeEtls = record.SnowflakeEtls;
            if (snowflakeEtls != null)
            {
                for (var i = 0; i < snowflakeEtls.Count; i++)
                {
                    if (snowflakeEtls[i].Name == ConfigurationName)
                    {
                        return snowflakeEtls[i];
                    }
                }
            }

            return null;
        }

        protected override UpdatedValue GetUpdatedValue(long index, RawDatabaseRecord record, ClusterOperationContext context, BlittableJsonReaderObject existingValue)
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

            if (DbId != null)
                etlState.LastProcessedEtagPerDbId[DbId] = LastProcessedEtag;

            etlState.ChangeVector = ChangeVector;
            etlState.NodeTag = NodeTag;
            etlState.SkippedTimeSeriesDocs = SkippedTimeSeriesDocs;
            etlState.LastBatchTime = LastBatchTime;

            return new UpdatedValue(UpdatedValueActionType.Update, context.ReadObject(etlState.ToJson(), GetItemId()));
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(ConfigurationName)] = ConfigurationName;
            json[nameof(TransformationName)] = TransformationName;
            json[nameof(LastProcessedEtag)] = LastProcessedEtag;
            json[nameof(ChangeVector)] = ChangeVector;
            json[nameof(NodeTag)] = NodeTag;
            json[nameof(HasHighlyAvailableTasks)] = HasHighlyAvailableTasks;
            json[nameof(DbId)] = DbId;
            json[nameof(SkippedTimeSeriesDocs)] = SkippedTimeSeriesDocs;
            json[nameof(LastBatchTime)] = LastBatchTime;
        }
    }
}
