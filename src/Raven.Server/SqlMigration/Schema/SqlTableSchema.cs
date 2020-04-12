using System.Collections.Generic;
using System.Linq;
using Raven.Client.Extensions;
using Raven.Server.SqlMigration.Model;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.Schema
{
    public class SqlTableSchema : IDynamicJson
    {
        public string Schema { get; set; }
        
        public string TableName { get; set; }
        
        public string DefaultQuery { get; set; }
        
        public List<TableColumn> Columns { get; set; } = new List<TableColumn>();
        
        public List<string> PrimaryKeyColumns { get; set; } = new List<string>();
        
        public List<TableReference> References { get; set; } = new List<TableReference>();

        public SqlTableSchema(string schema, string tableName, string defaultQuery)
        {
            Schema = schema;
            TableName = tableName;
            DefaultQuery = defaultQuery;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Schema)] = Schema,
                [nameof(TableName)] = TableName,
                [nameof(DefaultQuery)] = DefaultQuery,
                [nameof(Columns)] = new DynamicJsonArray(Columns.Select(x => x.ToJson())),
                [nameof(PrimaryKeyColumns)] = TypeConverter.ToBlittableSupportedType(PrimaryKeyColumns),
                [nameof(References)] = new DynamicJsonArray(References.Select(x => x.ToJson()))
            };
        }
        
        public TableReference FindReference(AbstractCollection collection, List<string> columns)
        {
            return References.FirstOrDefault(x => x.Table == collection.SourceTableName
                                                   && x.Schema == collection.SourceTableSchema
                                                   && EnumerableExtension.ContentEquals(x.Columns, columns));
        }
    }
}
