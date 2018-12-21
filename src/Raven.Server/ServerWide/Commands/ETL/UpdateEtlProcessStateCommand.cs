using System;
using System.Linq;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Json.Converters;
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

        private UpdateEtlProcessStateCommand() : base(null)
        {
            // for deserialization
        }

        public UpdateEtlProcessStateCommand(string databaseName, string configurationName, string transformationName, long lastProcessedEtag, string changeVector,
            string nodeTag, bool hasHighlyAvailableTasks) : base(databaseName)
        {
            ConfigurationName = configurationName;
            TransformationName = transformationName;
            LastProcessedEtag = lastProcessedEtag;
            ChangeVector = changeVector;
            NodeTag = nodeTag;
            HasHighlyAvailableTasks = hasHighlyAvailableTasks;
        }

        public override string GetItemId()
        {
            return EtlProcessState.GenerateItemName(DatabaseName, ConfigurationName, TransformationName);
        }

        private IDatabaseTask GetMatchingConfiguration(DatabaseRecord record)
        {
            var taskName = GetItemId();
            for (var i=0; i< record.RavenEtls.Count; i++)
            {
                if (record.RavenEtls[i].Name == ConfigurationName)
                {
                    return record.RavenEtls[i];
                }
            }

            for (var i = 0; i < record.SqlEtls.Count; i++)
            {
                if (record.SqlEtls[i].Name == ConfigurationName)
                {
                    return record.SqlEtls[i];
                }
            }

            return null;
        }

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, DatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue, RachisState state)
        {
            EtlProcessState etlState;

            if (existingValue != null)
            {
                etlState = JsonDeserializationClient.EtlProcessState(existingValue);

                var databaseTask = GetMatchingConfiguration(record);

                if (databaseTask == null)
                    throw new RachisApplyException($"Can't update progress of ETL {ConfigurationName} by node {NodeTag}, because it's configuration can't be found");


                var lastResponsibleNode = GetLastResponsibleNode(HasHighlyAvailableTasks, record.Topology, NodeTag);
                if (record.Topology.WhoseTaskIsIt(state, databaseTask, lastResponsibleNode) != NodeTag)
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
        }
    }
}
