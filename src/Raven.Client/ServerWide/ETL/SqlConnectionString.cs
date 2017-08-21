using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.ETL
{
    public class SqlConnectionString : ConnectionString
    {
        public string ConnectionString { get; set; }

        public override ConnectionStringType Type => ConnectionStringType.Sql;

        protected override void ValidateImpl(ref List<string> errors)
        {
            if (string.IsNullOrEmpty(ConnectionString))
                errors.Add($"{nameof(ConnectionString)} cannot be empty");
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(ConnectionString)] = ConnectionString;
            return json;
        }
    }
}