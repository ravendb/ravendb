using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations.Integrations.PostgreSQL
{
    public class PostgreSqlUser : IDynamicJson
    {
        public string Username { get; set; }
        public string Password { get; set; }
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Username)] = Username,
                [nameof(Password)] = Password
            };
        }
    }
}
