using System;
using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.Model
{
    public class SqlMigrationDocument
    {
        public string Id { get; set; }
        public string TableName { get; set; }
        public string Collection { get; set; }
        public DynamicJsonValue SpecialColumnsValues { get; set; }
        public DynamicJsonValue Object { get; set; }

        public SqlMigrationDocument(string tableName)
        {
            TableName = tableName;
            SpecialColumnsValues = new DynamicJsonValue();
            Object = new DynamicJsonValue();
        }

        public void SetCollection(string collectionName)
        {
            Object["@metadata"] = new DynamicJsonValue
            {
                ["@collection"] = collectionName
            };
            Collection = collectionName;
        }

        public BlittableJsonReaderObject ToBllitable(JsonOperationContext context)
        {
            try
            {
                return context.ReadObject(Object, Id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Cannot build document from table '{TableName}'. Document ID: {Id}", e);
            }
        }
    }   
}
