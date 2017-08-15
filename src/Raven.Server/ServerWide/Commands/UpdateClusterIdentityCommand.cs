using System;
using System.Collections.Generic;
using Raven.Client.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class UpdateClusterIdentityCommand : CommandBase
    {
        public string DatabaseName { get; set; }
        public Dictionary<string,long> Identities { get; set; }

        public UpdateClusterIdentityCommand()
        {            
        }

        public UpdateClusterIdentityCommand(string databaseName, IDictionary<string, long> identities)
        {
            DatabaseName = databaseName;
            Identities = new Dictionary<string, long>(identities);
        }

        public void ApplyIdentityValues(Dictionary<string, long> existingIdentities)
        {
            foreach (var kvp in Identities)
            {
                if (existingIdentities.TryGetValue(kvp.Key, out var existingValue))
                {
                    existingIdentities[kvp.Key] = Math.Max(existingValue, kvp.Value);
                }
                else
                {
                    existingIdentities.Add(kvp.Key, kvp.Value);
                }
            }
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var json = base.ToJson(context);

            FillJson(json);
            return json;
        }

        private void FillJson(DynamicJsonValue json)
        {
            json[nameof(DatabaseName)] = DatabaseName;
            json[nameof(Identities)] = (Identities ?? new Dictionary<string, long>()).ToJson();
        }
    }
}
