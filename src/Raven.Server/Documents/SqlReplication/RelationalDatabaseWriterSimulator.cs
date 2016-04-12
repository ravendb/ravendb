using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace Raven.Server.Documents.SqlReplication
{
    public class RelationalDatabaseWriterSimulator : RelationalDatabaseWriterBase
    {
        private readonly DbProviderFactory providerFactory;
        private readonly SqlReplication _sqlReplication;
        private readonly DbCommandBuilder commandBuilder;

        public RelationalDatabaseWriterSimulator(PredefinedSqlConnection predefinedSqlConnection, SqlReplication sqlReplication) 
            : base(predefinedSqlConnection)
        {
            _sqlReplication = sqlReplication;
            providerFactory = DbProviderFactories.GetFactory(predefinedSqlConnection.FactoryName);
            commandBuilder = providerFactory.CreateCommandBuilder();
        }

        public IEnumerable<string> SimulateExecuteCommandText(SqlReplicationScriptResult scriptResult)
        {
            foreach (var sqlReplicationTable in _sqlReplication.Configuration.SqlReplicationTables)
            {
                if (sqlReplicationTable.InsertOnlyMode)
                    continue;

                // first, delete all the rows that might already exist there
                foreach (string deleteQuery in GenerateDeleteItemsCommandText(sqlReplicationTable.TableName, sqlReplicationTable.DocumentKeyColumn, _sqlReplication.Configuration.ParameterizeDeletesDisabled,
                    scriptResult.Keys))
                {
                    yield return deleteQuery;
                }
            }

            foreach (var sqlReplicationTable in _sqlReplication.Configuration.SqlReplicationTables)
            {
                List<ItemToReplicate> dataForTable;
                if (scriptResult.Data.TryGetValue(sqlReplicationTable.TableName, out dataForTable) == false)
                    continue;

                foreach (string insertQuery in GenerteInsertItemCommandText(sqlReplicationTable.TableName, sqlReplicationTable.DocumentKeyColumn, dataForTable))
                {
                    yield return insertQuery;
                }
            }
        }

        private IEnumerable<string> GenerteInsertItemCommandText(string tableName, string pkName, List<ItemToReplicate> dataForTable)
        {
            foreach (var itemToReplicate in dataForTable)
            {

                var sb = new StringBuilder("INSERT INTO ")
                        .Append(GetTableNameString(tableName))
                        .Append(" (")
                        .Append(commandBuilder.QuoteIdentifier(pkName))
                        .Append(", ");
                foreach (var column in itemToReplicate.Columns)
                {
                    if (column.Key == pkName)
                        continue;
                    sb.Append(commandBuilder.QuoteIdentifier(column.Key)).Append(", ");
                }
                sb.Length = sb.Length - 2;


                sb.Append(") VALUES (")
                    .Append(itemToReplicate.DocumentKey)
                    .Append(", ");

                foreach (var column in itemToReplicate.Columns)
                {
                    if (column.Key == pkName)
                        continue;
                     DbParameter param = new SqlParameter();
                     RelationalDatabaseWriter.SetParamValue(param, column, null);
                     sb.Append("'").Append(param.Value).Append("'").Append(", ");
                }
                sb.Length = sb.Length - 2;
                sb.Append(")");
                if (IsSqlServerFactoryType && _sqlReplication.Configuration.ForceSqlServerQueryRecompile)
                {
                    sb.Append(" OPTION(RECOMPILE)");
                }

                sb.Append(";");

                yield return sb.ToString();
            }
        }

        private IEnumerable<string> GenerateDeleteItemsCommandText(string tableName, string pkName, bool doNotParameterize, List<string> identifiers)
        {
            const int maxParams = 1000;

           _sqlReplication.CancellationToken.ThrowIfCancellationRequested();
            for (int i = 0; i < identifiers.Count; i += maxParams)
            {

                var sb = new StringBuilder("DELETE FROM ")
                    .Append(GetTableNameString(tableName))
                    .Append(" WHERE ")
                    .Append(commandBuilder.QuoteIdentifier(pkName))
                    .Append(" IN (");

                for (int j = i; j < Math.Min(i + maxParams, identifiers.Count); j++)
                {
                    if (i != j)
                        sb.Append(", ");
                    if (doNotParameterize == false)
                    {
                        sb.Append(identifiers[j]);
                    }
                    else
                    {
                        sb.Append("'").Append(RelationalDatabaseWriter.SanitizeSqlValue(identifiers[j])).Append("'");
                    }

                }
                sb.Append(")");

                if (IsSqlServerFactoryType && _sqlReplication.Configuration.ForceSqlServerQueryRecompile)
                {
                    sb.Append(" OPTION(RECOMPILE)");
                }

                sb.Append(";");
                yield return sb.ToString();
            }


        }

        private string GetTableNameString(string tableName)
        {
            if (_sqlReplication.Configuration.QuoteTables)
            {
                return string.Join(".", tableName.Split('.').Select(x => commandBuilder.QuoteIdentifier(x)).ToArray());
            }
            else
            {
                return tableName;
            }
        }
    }
}