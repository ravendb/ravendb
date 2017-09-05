using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration
{
    public class RavenDocument : DynamicJsonValue
    {
        public string Id;
        public string TableName;

        public RavenDocument(string tableName)
        {
            Id = string.Empty;
            TableName = tableName;
        }   

        public void Set(string key, object value, bool trimStrings = true)
        {
            switch (value)
            {
                case string _:
                    if (trimStrings)
                        this[key] = value.ToString().Trim();
                    else
                        this[key] = value.ToString();
                    return;
                case DBNull _:
                    this[key] = null;
                    return;
                case byte[] _:
                    this[key] = System.Convert.ToBase64String((byte[])value);
                    return;
                case Guid _:
                    this[key] = value.ToString();
                    return;
            }

            this[key] = value;
        }

        public void Append(string key, RavenDocument ravenDocument)
        {
            if (this[key] == null)
                this[key] = new List<RavenDocument>();

            var lst = (List<RavenDocument>)this[key];
            lst.Add(ravenDocument);
        }

        public void SetCollection(string collectionName)
        {
            this["@metadata"] = new DynamicJsonValue
            {
                ["@collection"] = collectionName
            };
        }
    }   
}
