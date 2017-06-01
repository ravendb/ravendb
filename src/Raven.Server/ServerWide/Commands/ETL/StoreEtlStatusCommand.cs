using Raven.Client.Server;
using Raven.Client.Server.ETL;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.ETL
{
    public class StoreEtlStatusCommand : UpdateValueForDatabaseCommand
    {
        public EtlProcessStatus EtlProcessStatus;

        private StoreEtlStatusCommand() : base(null)
        {
            // for deserialization
        }

        public StoreEtlStatusCommand(EtlProcessStatus status, string databaseName) : base(databaseName)
        {
            EtlProcessStatus = status;
        }

        public override string GetItemId()
        {
            return EtlProcessStatus.GenerateItemName(DatabaseName, EtlProcessStatus.Destination, EtlProcessStatus.TransformationName);
        }

        public override BlittableJsonReaderObject GetUpdatedValue(long index, DatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue)
        {
            return context.ReadObject(EtlProcessStatus.ToJson(), GetItemId());
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(EtlProcessStatus)] = EtlProcessStatus.ToJson();
        }
    }
}