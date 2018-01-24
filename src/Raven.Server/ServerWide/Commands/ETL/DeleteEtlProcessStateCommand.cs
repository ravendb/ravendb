using Raven.Client.Documents.Operations.ETL;
using Raven.Client.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.ETL
{
    public class RemoveEtlProcessStateCommand : UpdateValueForDatabaseCommand
    {
        public string ConfigurationName { get; set; }

        public string TransformationName { get; set; }

        private RemoveEtlProcessStateCommand() : base(null)
        {
            // for deserialization
        }

        public RemoveEtlProcessStateCommand(string databaseName, string configurationName, string transformationName) : base(databaseName)
        {
            ConfigurationName = configurationName;
            TransformationName = transformationName;
        }

        public override string GetItemId()
        {
            return EtlProcessState.GenerateItemName(DatabaseName, ConfigurationName, TransformationName);
        }

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, DatabaseRecord record, JsonOperationContext context,
            BlittableJsonReaderObject existingValue, RachisState state)
        {
            return null; // it's going to delete the value
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(ConfigurationName)] = ConfigurationName;
            json[nameof(TransformationName)] = TransformationName;
        }
    }
}
