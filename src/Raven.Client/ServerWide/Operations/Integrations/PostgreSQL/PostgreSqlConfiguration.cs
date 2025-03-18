using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations.Integrations.PostgreSQL
{
    /// <summary>
    /// The configuration for PostgreSQL integration within RavenDB
    /// </summary>
    public sealed class PostgreSqlConfiguration : IDynamicJson
    {
        /// <summary>
        /// The authentication settings for connecting to the PostgreSQL database.
        /// </summary>
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
