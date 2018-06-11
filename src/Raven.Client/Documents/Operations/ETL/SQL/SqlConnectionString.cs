using System.Collections.Generic;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.SQL
{
    public class SqlConnectionString : ConnectionString
    {
        public string ConnectionString { get; set; }

        public string FactoryName { get; set; }

        public override ConnectionStringType Type => ConnectionStringType.Sql;

        protected override void ValidateImpl(ref List<string> errors)
        {
            if (string.IsNullOrEmpty(ConnectionString))
                errors.Add($"{nameof(ConnectionString)} cannot be empty");
        }

        public override bool IsEqual(ConnectionString connectionString)
        {
            if (connectionString is SqlConnectionString sqlConnection)
            {
                return base.IsEqual(connectionString) && ConnectionString == sqlConnection.ConnectionString;
            }

            return false;
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(ConnectionString)] = ConnectionString;
            json[nameof(FactoryName)] = FactoryName;

            return json;
        }
    }
}
