using System.Collections.Generic;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class IncrementClusterIdentityCommand : UpdateDatabaseCommand
    {
        public string Prefix { get; set; }

        public IncrementClusterIdentityCommand() : base(null)
        {
        }

        public IncrementClusterIdentityCommand(string databaseName) : base(databaseName)
        {
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (record.Identities == null)
                record.Identities = new Dictionary<string, long>();

            record.Identities.TryGetValue(Prefix, out var prev);
            record.Identities[Prefix] = prev + 1;

            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Prefix)] = Prefix;
        }
    }
}
