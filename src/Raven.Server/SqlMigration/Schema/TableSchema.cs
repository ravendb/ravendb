using System.Collections.Generic;
using System.Linq;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.Schema
{
    public class TableSchema : IDynamicJson
    {
        public List<TableColumn> Columns { get; set; } = new List<TableColumn>();
        
        public List<string> PrimaryKeyColumns { get; set; } = new List<string>();
        
        public List<TableReference> References { get; set; } = new List<TableReference>();

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Columns)] = new DynamicJsonArray(Columns.Select(x => x.ToJson())),
                [nameof(PrimaryKeyColumns)] = TypeConverter.ToBlittableSupportedType(PrimaryKeyColumns),
                [nameof(References)] = new DynamicJsonArray(References.Select(x => x.ToJson()))
            };
        }
    }
}
