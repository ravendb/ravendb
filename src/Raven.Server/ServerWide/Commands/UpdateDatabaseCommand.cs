using Raven.Client.Server;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Commands
{
    public abstract class UpdateDatabaseCommand : CommandBase
    {
        public string DatabaseName;        

        protected UpdateDatabaseCommand(string databaseName)
        {
            DatabaseName = databaseName;
        }

        public abstract string UpdateDatabaseRecord(DatabaseRecord record, long etag);

        public abstract void FillJson(DynamicJsonValue json);

        public override DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue
            {
                ["Type"] = GetType().Name,
                [nameof(DatabaseName)] = DatabaseName
            };

            FillJson(json);

            return json;
        }
    }

    public abstract class CommandBase
    {
        public abstract DynamicJsonValue ToJson();
        public long? Etag;
    }
}