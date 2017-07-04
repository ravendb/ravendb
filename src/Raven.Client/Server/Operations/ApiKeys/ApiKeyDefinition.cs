using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Server.Operations.ApiKeys
{
    public class ApiKeyDefinition
    {
        public bool Enabled;
        public string Secret;
        public bool ServerAdmin;
        public Dictionary<string, AccessMode> ResourcesAccessMode = new Dictionary<string, AccessMode>(StringComparer.OrdinalIgnoreCase);

        public virtual DynamicJsonValue ToJson()
        {
            var ram = new DynamicJsonValue();
            foreach (var kvp in ResourcesAccessMode)
                ram[kvp.Key] = kvp.Value.ToString();

            return new DynamicJsonValue
            {
                [nameof(Enabled)] = Enabled,
                [nameof(Secret)] = Secret,
                [nameof(ServerAdmin)] = ServerAdmin,
                [nameof(Enabled)] = Enabled,
                [nameof(ResourcesAccessMode)] = ram
            };
        }
    }


    public enum AccessMode
    {
        None,
        ReadOnly,
        ReadWrite,
        Admin
    }

    public class NamedApiKeyDefinition : ApiKeyDefinition
    {
        public string UserName;

        public override DynamicJsonValue ToJson()
        {
            var djv = base.ToJson();
            djv[nameof(UserName)] = UserName;

            return djv;
        }
    }
}
