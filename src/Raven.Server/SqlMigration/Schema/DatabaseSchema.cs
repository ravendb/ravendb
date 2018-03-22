using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.Schema
{
    public class DatabaseSchema : IDynamicJson
    {
        public string CatalogName { get; set; }
        public List<TableSchema> Tables { get; set; } = new List<TableSchema>();

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(CatalogName)] = CatalogName,
                [nameof(Tables)] = new DynamicJsonArray(Tables.Select(x => x.ToJson()))
            };
        }

        public TableSchema GetTable(string schema, string tableName)
        {
            return Tables.FirstOrDefault(x => x.Schema == schema && x.TableName == tableName);
        }
    }
}
