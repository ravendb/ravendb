using Raven.Client.Server;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public abstract class UpdateValueForDatabaseCommand : CommandBase
    {
        public abstract string GetItemId();
        public abstract BlittableJsonReaderObject GetUpdatedValue(long index, DatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue);
        public abstract void FillJson(DynamicJsonValue json);
        public string DatabaseName { get; set; }

        protected UpdateValueForDatabaseCommand(string databaseName)
        {
            DatabaseName = databaseName;
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            djv[nameof(DatabaseName)] = DatabaseName;

            FillJson(djv);

            return djv;
        }
    }
}
