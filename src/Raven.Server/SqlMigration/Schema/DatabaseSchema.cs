using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.Schema
{
    public class DatabaseSchema : IDynamicJson
    {
        public string CatalogName { get; set; }
        public List<SqlTableSchema> Tables { get; set; } = new List<SqlTableSchema>();

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(CatalogName)] = CatalogName,
                [nameof(Tables)] = new DynamicJsonArray(Tables.Select(x => x.ToJson()))
            };
        }

        public SqlTableSchema GetTable(string schema, string tableName)
        {
            return Tables.FirstOrDefault(x => x.Schema == schema && x.TableName == tableName);
        }
        
        public HashSet<string> FindSpecialColumns(string tableSchema, string tableName)
        {
            var mainSchema = GetTable(tableSchema, tableName);

            var result = new HashSet<string>();
            mainSchema.PrimaryKeyColumns.ForEach(x => result.Add(x));

            foreach (var fkCandidate in Tables)
            foreach (var tableReference in fkCandidate.References
                .Where(x => x.Table == tableName && x.Schema == tableSchema))
            {
                tableReference.Columns.ForEach(x => result.Add(x));
            }

            return result;
        }
    }
}
