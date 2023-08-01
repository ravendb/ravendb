using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.Schema
{
    public sealed class TableColumn : IDynamicJson
    {
        public string Name { get; set; }
        public ColumnType Type { get; set; }

        public TableColumn(ColumnType type, string name)
        {
            Type = type;
            Name = name;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Type)] = Type,
                [nameof(Name)] = Name
            };
        }
    }
}
