using System;
using System.Collections.Generic;
using Raven.Client;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.Model
{
    public class SqlMigrationDocument
    {
        public string Id { get; set; }
        public string Collection { get; set; }
        public DynamicJsonValue SpecialColumnsValues { get; set; }
        public DynamicJsonValue Object { get; set; }
        public Dictionary<string, byte[]> Attachments;

        public SqlMigrationDocument()
        {
            SpecialColumnsValues = new DynamicJsonValue();
            Object = new DynamicJsonValue();
        }

        public void SetCollection(string collectionName)
        {
            Object[Constants.Documents.Metadata.Key] = new DynamicJsonValue
            {
                [Constants.Documents.Metadata.Collection] = collectionName
            };
            Collection = collectionName;
        }

        public BlittableJsonReaderObject ToBlittable(JsonOperationContext context)
        {
            try
            {
                return context.ReadObject(Object, Id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Cannot build document with ID: {Id}", e);
            }
        }
    }
}
