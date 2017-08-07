using System;
using System.Collections.Generic;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class UpdateClusterIdentityCommand : UpdateDatabaseCommand
    {
        public Dictionary<string,long> Identities { get; set; }

        public UpdateClusterIdentityCommand() : base(null)
        {            
        }

        public UpdateClusterIdentityCommand(string databaseName, IDictionary<string, long> identities) : base(databaseName)
        {
            Identities = new Dictionary<string, long>(identities);
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            
            foreach (var kvp in Identities)
            {
                if (record.Identities.TryGetValue(kvp.Key, out long existingValue))
                {
                    record.Identities[kvp.Key] = Math.Max(existingValue, kvp.Value);
                }
                else
                {
                    record.Identities.Add(kvp.Key, kvp.Value);
                }
            }
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            if(Identities == null)
                Identities = new Dictionary<string, long>();

            json[nameof(Identities)] = Identities.ToJson();
        }
    }
}
