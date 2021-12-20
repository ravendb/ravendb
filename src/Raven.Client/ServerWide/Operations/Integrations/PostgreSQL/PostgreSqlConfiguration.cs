using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations.Integrations.PostgreSQL
{
    public class PostgreSqlConfiguration : IDynamicJson
    {
        public PostgreSqlAuthenticationConfiguration Authentication;
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue()
            {
                [nameof(Authentication)] = Authentication.ToJson()
            };
        }
    }
}
