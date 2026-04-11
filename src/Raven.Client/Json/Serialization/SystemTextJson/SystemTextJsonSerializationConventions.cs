using Raven.Client.Documents.Conventions;

namespace Raven.Client.Json.Serialization.SystemTextJson
{
    // Minimal stub - will be filled in when the full STJ serialization conventions are implemented.
    internal sealed class SystemTextJsonSerializationConventions
    {
        public DocumentConventions Conventions { get; set; }

        public bool IgnoreByRefMembers { get; set; }

        public bool IgnoreUnsafeMembers { get; set; }
    }
}
