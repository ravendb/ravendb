using System.Collections.Generic;

namespace Raven.Client.Server.ETL
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
    }
}