using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.SQL
{
    public class SqlDestination : EtlDestination
    {
        public SqlDestination()
        {
            SqlTables = new List<SqlEtlTable>();
        }

        public SqlEtlConnection Connection { get; set; }

        public bool ParameterizeDeletes { get; set; } = true;

        public bool ForceQueryRecompile { get; set; }

        public bool QuoteTables { get; set; } = true;

        public int? CommandTimeout { get; set; }

        public List<SqlEtlTable> SqlTables { get; set; }

        public override bool Validate(ref List<string> errors)
        {
            if (string.IsNullOrEmpty(Connection.FactoryName))
                errors.Add($"{nameof(Connection.FactoryName)} cannot be empty");

            if (string.IsNullOrEmpty(Connection.ConnectionString))
                errors.Add($"{nameof(Connection.ConnectionString)} cannot be empty");

            if (SqlTables.Count == 0)
                errors.Add($"{nameof(SqlTables)} cannot be empty");

            return errors.Count == 0;
        }

        public override string ToString()
        {
            return $"{Connection.ConnectionString}";
        }
    }

    public class SqlEtlTable
    {
        public string TableName { get; set; }
        public string DocumentKeyColumn { get; set; }
        public bool InsertOnlyMode { get; set; }

        protected bool Equals(SqlEtlTable other)
        {
            return string.Equals(TableName, other.TableName) && string.Equals(DocumentKeyColumn, other.DocumentKeyColumn, StringComparison.OrdinalIgnoreCase) &&
                   InsertOnlyMode == other.InsertOnlyMode;
        }
    }
}