using Raven.Client.Server;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Commands
{
    public abstract class UpdateDatabaseCommand
    {
        public string DatabaseName;

        public long? Etag;

        protected UpdateDatabaseCommand(string databaseName)
        {
            DatabaseName = databaseName;
        }

        public abstract string UpdateDatabaseRecord(DatabaseRecord record, long etag);

        public abstract void FillJson(DynamicJsonValue json);

        public DynamicJsonValue ToJson()
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
}