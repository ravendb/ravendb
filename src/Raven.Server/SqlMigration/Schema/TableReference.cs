using System.Collections.Generic;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.Schema
{
    public class TableReference : IDynamicJson
    {
        public string Table { get; set; }
        public List<string> Columns { get; set; } = new List<string>();

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Table)] = Table,
                [nameof(Columns)] = TypeConverter.ToBlittableSupportedType(Columns)
            };
        }
    }
}
