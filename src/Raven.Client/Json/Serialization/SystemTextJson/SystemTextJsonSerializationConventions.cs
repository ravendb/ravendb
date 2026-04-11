using System.Text.Json;
using System.Text.Json.Serialization;
using Raven.Client.Documents.Conventions;

namespace Raven.Client.Json.Serialization.SystemTextJson
{
    // Minimal stub - will be filled in when the full STJ serialization conventions are implemented.
    internal sealed class SystemTextJsonSerializationConventions
    {
        public DocumentConventions Conventions { get; set; }

        public bool IgnoreByRefMembers { get; set; }

        public bool IgnoreUnsafeMembers { get; set; }

        internal JsonSerializerOptions CreateJsonSerializerOptions()
        {
            var options = new JsonSerializerOptions
            {
                TypeInfoResolver = new RavenJsonTypeInfoResolver(this),
                NumberHandling = JsonNumberHandling.AllowReadingFromString,
                PropertyNameCaseInsensitive = true,
                DefaultIgnoreCondition = JsonIgnoreCondition.Never
            };

            return options;
        }
    }
}
