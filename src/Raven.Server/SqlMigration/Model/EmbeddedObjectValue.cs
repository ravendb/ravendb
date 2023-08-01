using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.Model
{
    public sealed class EmbeddedObjectValue
    {
        public DynamicJsonValue Object { get; set; }
        public DynamicJsonValue SpecialColumnsValues { get; set; }
        public Dictionary<string, byte[]> Attachments { get; set; }
    }
}
