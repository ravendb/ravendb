using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class AddDatabaseCommand : CommandBase
    {
        public string Name;
        public BlittableJsonReaderObject Record;
        public bool Encrypted;
        public override DynamicJsonValue ToJson()
        {
            //this is just for validating that this is a valid database record
            JsonDeserializationCluster.DatabaseRecord(Record);
            return new DynamicJsonValue
            {
                ["Type"] = nameof(AddDatabaseCommand),
                [nameof(Name)] = Name,
                [nameof(Record)] = Record,
                [nameof(Etag)] = Etag,
                [nameof(Encrypted)] = Encrypted
            };
        }
    }

}
