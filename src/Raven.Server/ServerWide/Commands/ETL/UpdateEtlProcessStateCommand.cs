using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide;
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

        private UpdateEtlProcessStateCommand() : base(null)
        {
            // for deserialization
        }

        public UpdateEtlProcessStateCommand(string databaseName, string configurationName, string transformationName, long lastProcessedEtag, string changeVector,
            string nodeTag) : base(databaseName)
        {
            ConfigurationName = configurationName;
            TransformationName = transformationName;
            LastProcessedEtag = lastProcessedEtag;
            ChangeVector = changeVector;
            NodeTag = nodeTag;
        }

        public override string GetItemId()
        {
            return EtlProcessState.GenerateItemName(DatabaseName, ConfigurationName, TransformationName);
        }

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, DatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue, RachisState state)
        {
            EtlProcessState etlState;

            if (existingValue != null)
                etlState = JsonDeserializationClient.EtlProcessState(existingValue);
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

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(ConfigurationName)] = ConfigurationName;
            json[nameof(TransformationName)] = TransformationName;
            json[nameof(LastProcessedEtag)] = LastProcessedEtag;
            json[nameof(ChangeVector)] = ChangeVector;
            json[nameof(NodeTag)] = NodeTag;
        }
    }
}
