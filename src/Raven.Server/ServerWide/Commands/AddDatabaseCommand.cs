using System.Collections.Generic;
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
        public Dictionary<string, BlittableJsonReaderObject> DatabaseValues;
        public bool Encrypted;
        public bool IsRestore;

        public override object FromRemote(object remoteResult)
        {
            var rc = new List<string>();
            var obj = remoteResult as BlittableJsonReaderArray;

            if (obj == null)
            {
                // this is an error as we expect BlittableJsonReaderArray, but we will pass the object value to get later appropriate exception
                return base.FromRemote(remoteResult);
            }

            foreach (var o in obj)
            {
                rc.Add(o.ToString());
            }
            return rc;
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            return new DynamicJsonValue
            {
                ["Type"] = nameof(AddDatabaseCommand),
                [nameof(Name)] = Name,
                [nameof(Record)] = EntityToBlittable.ConvertCommandToBlittable(Record, context),
                [nameof(RaftCommandIndex)] = RaftCommandIndex,
                [nameof(Encrypted)] = Encrypted,
                [nameof(DatabaseValues)] = DynamicJsonValue.Convert(DatabaseValues),
                [nameof(IsRestore)] = IsRestore
            };
        }
    }
}
