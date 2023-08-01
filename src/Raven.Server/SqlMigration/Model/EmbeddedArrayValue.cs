using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.Model
{
    public sealed class EmbeddedArrayValue
    {
        public DynamicJsonArray ArrayOfNestedObjects { get; set; }
        public List<DynamicJsonValue> SpecialColumnsValues { get; set; }
        public List<Dictionary<string, byte[]>> Attachments { get; set; }
    }
}
