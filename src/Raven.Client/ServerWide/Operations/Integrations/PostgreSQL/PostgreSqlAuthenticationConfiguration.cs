using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations.Integrations.PostgreSQL
{
    public class PostgreSqlAuthenticationConfiguration : IDynamicJson
    {
        public List<PostgreSqlUser> Users = new();

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Users)] = new DynamicJsonArray(Users.Select(x => x.ToJson()))
            };
        }
    }
}
