using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Server.Config.Categories;
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
            if (TryGetMatchingConfiguration(record.RavenEtls, out var ravenEtlConfiguration))
                return ravenEtlConfiguration;

            if (TryGetMatchingConfiguration(record.SqlEtls, out var sqlEtlConfiguration))
                return sqlEtlConfiguration;

            if (TryGetMatchingConfiguration(record.OlapEtls, out var olapEtlConfiguration))
                return olapEtlConfiguration;

            if (TryGetMatchingConfiguration(record.ElasticSearchEtls, out var elasticEtlConfiguration))
                return elasticEtlConfiguration;

            if (TryGetMatchingConfiguration(record.QueueEtls, out var queueEtlConfiguration))
                return queueEtlConfiguration;

            if (TryGetMatchingConfiguration(record.SnowflakeEtls, out var snowflakeEtlConfiguration))
                return snowflakeEtlConfiguration;

            if (TryGetMatchingConfiguration(record.AiEtls, out var aiEtlConfiguration))
                return aiEtlConfiguration;

            return null;

            bool TryGetMatchingConfiguration<T>(List<T> tasks, out T matched) where T : IDatabaseTask
            {
                foreach (var task in tasks.Where(t => t.GetTaskName() == ConfigurationName))
                {
                    matched = task;
                    return true;
                }

                matched = default;
                return false;
            }
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
