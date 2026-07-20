using System.Linq;
using JetBrains.Annotations;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public sealed class ModifyDatabaseSupportedFeaturesCommand : UpdateDatabaseCommand
    {
        public string[] Add;
        public string[] Remove;

        [UsedImplicitly(Reason = "For deserialization purpose")]
        public ModifyDatabaseSupportedFeaturesCommand()
        {
        }

        public ModifyDatabaseSupportedFeaturesCommand(string databaseName, string[] add, string[] remove, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            Add = add;
            Remove = remove;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            var supportedFeatures = record.SupportedFeatures?.ToList() ?? [];

            if (Add != null)
            {
                foreach (var feature in Add)
                {
                    if (feature != null && supportedFeatures.Contains(feature) == false)
                        supportedFeatures.Add(feature);
                }
            }

            if (Remove != null)
            {
                foreach (var feature in Remove)
                    supportedFeatures.RemoveAll(x => x == feature);
            }

            record.SupportedFeatures = supportedFeatures;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Add)] = Add;
            json[nameof(Remove)] = Remove;
        }

        public sealed class Parameters
        {
            public string[] Add;
            public string[] Remove;
        }
    }
}
