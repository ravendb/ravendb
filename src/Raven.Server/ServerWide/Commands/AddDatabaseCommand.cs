using System.Collections.Generic;
using System.Dynamic;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class AddDatabaseCommand : CommandBase
    {
        public string Name;
        public DatabaseRecord Record;
        public Dictionary<string, ExpandoObject> DatabaseValues;
        public bool Encrypted;
        public bool IsRestore;

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            return new DynamicJsonValue
            {
                ["Type"] = nameof(AddDatabaseCommand),
                [nameof(Name)] = Name,
                [nameof(Record)] = EntityToBlittable.ConvertCommandToBlittable(Record, context),
                [nameof(RaftCommandIndex)] = RaftCommandIndex,
                [nameof(Encrypted)] = Encrypted,
                [nameof(DatabaseValues)] = EntityToBlittable.ConvertCommandToBlittable(DatabaseValues, context),
                [nameof(IsRestore)] = IsRestore
            };
        }
    }
}
