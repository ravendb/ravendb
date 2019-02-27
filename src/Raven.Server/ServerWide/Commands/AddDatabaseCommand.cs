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
            DynamicJsonValue databaseValues = null;
            if (DatabaseValues != null)
            {
                databaseValues = new DynamicJsonValue();
                foreach (var kvp in DatabaseValues)
                    databaseValues[kvp.Key] = kvp.Value?.Clone(context);
            }

            var djv = base.ToJson(context);
            djv[nameof(Name)] = Name;
            djv[nameof(Record)] = EntityToBlittable.ConvertCommandToBlittable(Record, context);
            djv[nameof(RaftCommandIndex)] = RaftCommandIndex;
            djv[nameof(Encrypted)] = Encrypted;
            djv[nameof(DatabaseValues)] = databaseValues;
            djv[nameof(IsRestore)] = IsRestore;

            return djv;
        }
    }
}
