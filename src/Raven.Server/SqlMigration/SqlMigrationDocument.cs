using System;
using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration
{
    public class SqlMigrationDocument : DynamicJsonValue
    {
        public string Id;
        public string TableName;

        public SqlMigrationDocument(string tableName = null)
        {
            Id = string.Empty;
            TableName = tableName;
        }

        public void Set(string key, object value, bool trimStrings = true)
        {
            switch (value)
            {
                case null:
                case DBNull _:
                    this[key] = null;
                    return;
                case string str:
                    if (trimStrings)
                        this[key] = str.Trim();
                    else
                        this[key] = str;
                    return;
                case byte[] byteArray:
                    this[key] = System.Convert.ToBase64String(byteArray);
                    return;
                case Guid guid:
                    this[key] = guid.ToString();
                    return;
            }

            this[key] = value;
        }

        public void Append(string key, SqlMigrationDocument sqlMigrationDocument)
        {
            if (this[key] == null)
                this[key] = new List<SqlMigrationDocument>();

            var lst = (List<SqlMigrationDocument>)this[key];
            lst.Add(sqlMigrationDocument);
        }

        public void SetCollection(string collectionName)
        {
            this["@metadata"] = new SqlMigrationDocument
            {
                ["@collection"] = collectionName
            };
        }

        public BlittableJsonReaderObject ToBllitable(JsonOperationContext context)
        {
            try
            {
                return context.ReadObject(this, Id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Cannot build raven document from table '{TableName}'. Raven document id is: {Id}", e);
            }
        }
    }   
}
