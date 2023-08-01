using System.Collections.Generic;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.Schema
{
    public sealed class TableReference : IDynamicJson
    {
        public string Schema { get; set; }
        public string Table { get; set; }
        public List<string> Columns { get; set; } = new List<string>();

        public TableReference(string schema, string table)
        {
            Schema = schema;
            Table = table;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Table)] = Table,
                [nameof(Schema)] = Schema,
                [nameof(Columns)] = TypeConverter.ToBlittableSupportedType(Columns)
            };
        }
    }
}
