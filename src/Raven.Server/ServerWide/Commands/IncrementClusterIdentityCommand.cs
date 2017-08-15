using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class IncrementClusterIdentityCommand : CommandBase
    {
        public string DatabaseName { get; set; }
        public string Prefix { get; set; }

        public IncrementClusterIdentityCommand()
        {
        }

        public IncrementClusterIdentityCommand(string databaseName)
        {
            DatabaseName = databaseName;
        }

        public void Increment(Dictionary<string,long> identities)
        {
            identities.TryGetValue(Prefix, out var prev);
            identities[Prefix] = prev + 1;
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
            json[nameof(Prefix)] = Prefix;
        }
    }
}
