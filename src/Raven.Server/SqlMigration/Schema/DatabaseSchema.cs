using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.Schema
{
    public class DatabaseSchema : IDynamicJson
    {
        public string Name { get; set; }
        public Dictionary<string, TableSchema> Tables { get; set; } = new Dictionary<string, TableSchema>();

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Tables)] = DynamicJsonValue.Convert(Tables)
            };
        }
    }
}
