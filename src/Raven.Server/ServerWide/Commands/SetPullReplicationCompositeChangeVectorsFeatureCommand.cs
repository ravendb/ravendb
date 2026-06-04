using System.Linq;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public sealed class SetPullReplicationCompositeChangeVectorsFeatureCommand : UpdateDatabaseCommand
    {
        public bool Enabled;

        [UsedImplicitly(Reason = "For deserialization purpose")]
        public SetPullReplicationCompositeChangeVectorsFeatureCommand()
        {
        }

        public SetPullReplicationCompositeChangeVectorsFeatureCommand(string databaseName, bool enabled, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            Enabled = enabled;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            var supportedFeatures = record.SupportedFeatures?.ToList() ?? [];

            if (Enabled)
            {
                if (supportedFeatures.Contains(Constants.DatabaseRecord.SupportedFeatures.PullReplicationCompositeChangeVectors) == false)
                    supportedFeatures.Add(Constants.DatabaseRecord.SupportedFeatures.PullReplicationCompositeChangeVectors);

                record.SupportedFeatures = supportedFeatures;
                return;
            }

            supportedFeatures.RemoveAll(x => x == Constants.DatabaseRecord.SupportedFeatures.PullReplicationCompositeChangeVectors);
            record.SupportedFeatures = supportedFeatures;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Enabled)] = Enabled;
        }
    }
}
