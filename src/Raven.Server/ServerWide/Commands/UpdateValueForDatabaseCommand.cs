using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents;
using Raven.Client.Server;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Raven.Server.ServerWide.Commands
{
    public abstract class UpdateValueForDatabaseCommand
    {
        public abstract string GetItemId();
        public abstract DynamicJsonValue GetUpdatedValue(long idnex, DatabaseRecord record, BlittableJsonReaderObject existingValue);
        public abstract void FillJson(DynamicJsonValue json);
        public string DatabaseName { get; set; }

        public UpdateValueForDatabaseCommand(string databaseName)
        {
            DatabaseName = databaseName;
        }

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
