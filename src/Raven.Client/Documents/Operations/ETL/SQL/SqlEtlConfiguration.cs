using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.SQL
{
    public class SqlEtlConfiguration : EtlConfiguration<SqlConnectionString>
    {
        private string _name;

        public SqlEtlConfiguration()
        {
            SqlTables = new List<SqlEtlTable>();
        }

        public string FactoryName { get; set; }

        public bool ParameterizeDeletes { get; set; } = true;

        public bool ForceQueryRecompile { get; set; }

        public bool QuoteTables { get; set; } = true;

        public int? CommandTimeout { get; set; }

        public List<SqlEtlTable> SqlTables { get; set; }

        public override EtlType EtlType => EtlType.Sql;

        public override bool Validate(out List<string> errors)
        {
            base.Validate(out errors);

            if (string.IsNullOrEmpty(FactoryName))
                errors.Add($"{nameof(FactoryName)} cannot be empty");

            if (SqlTables.Count == 0)
                errors.Add($"{nameof(SqlTables)} cannot be empty");

            return errors.Count == 0;
        }

        public override string GetDestination()
        {
            if (_name != null)
                return _name;

            var (database, server) = SqlConnectionStringParser.GetDatabaseAndServerFromConnectionString(FactoryName, Connection.ConnectionString);

            return _name = $"{database}@{server}";
        }

        public override bool UsingEncryptedCommunicationChannel()
        {
            switch (SqlProviderParser.GetSupportedProvider(FactoryName))
            {
                case SqlProvider.SqlClient:
                    var encrypt = SqlConnectionStringParser.GetConnectionStringValue(Connection.ConnectionString, new[] {"Encrypt"});

                    if (string.IsNullOrEmpty(encrypt))
                        return false;

                    if (bool.TryParse(encrypt, out var encryptBool) == false)
                        return false;

                    return encryptBool;
                case SqlProvider.Npgsql:
                    var sslMode = SqlConnectionStringParser.GetConnectionStringValue(Connection.ConnectionString, new[] { "SslMode" });

                    if (string.IsNullOrEmpty(sslMode))
                        return false;

                    switch (sslMode.ToLower())
                    {
                        case "require":
                        case "verify-ca":
                        case "verify-full":
                            return true;
                    }

                    return false;
                default:
                    throw new NotSupportedException($"Factory '{FactoryName}' is not supported");
            }
        }

        public override string GetDefaultTaskName()
        {
            return $"SQL ETL to {ConnectionStringName}";
        }

        public override DynamicJsonValue ToJson()
        {
            var result = base.ToJson();

            result[nameof(FactoryName)] = FactoryName;
            result[nameof(ParameterizeDeletes)] = ParameterizeDeletes;
            result[nameof(ForceQueryRecompile)] = ForceQueryRecompile;
            result[nameof(QuoteTables)] = QuoteTables;
            result[nameof(CommandTimeout)] = CommandTimeout;
            result[nameof(SqlTables)] = new DynamicJsonArray(SqlTables.Select(x => x.ToJson()));
            
            return result;
        }
    }

    public class SqlEtlTable
    {
        public string TableName { get; set; }
        public string DocumentIdColumn { get; set; }
        public bool InsertOnlyMode { get; set; }

        protected bool Equals(SqlEtlTable other)
        {
            return string.Equals(TableName, other.TableName) && string.Equals(DocumentIdColumn, other.DocumentIdColumn, StringComparison.OrdinalIgnoreCase) &&
                   InsertOnlyMode == other.InsertOnlyMode;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(TableName)] = TableName,
                [nameof(DocumentIdColumn)] = DocumentIdColumn,
                [nameof(InsertOnlyMode)] = InsertOnlyMode
            };
        }
    }
}
