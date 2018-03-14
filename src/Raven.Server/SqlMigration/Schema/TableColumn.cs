using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.Schema
{
    public class TableColumn : IDynamicJson
    {
        public string Name { get; set; }
        public ColumnType Type { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Type)] = Type
            };
        }
    }
}
