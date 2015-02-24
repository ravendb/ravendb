using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.Odbc;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Database.Bundles.SqlReplication
{
    public class RelationalDatabaseWriterSimulator
    {
        private DocumentDatabase database;
        private SqlReplicationConfig cfg;
        private readonly DbProviderFactory providerFactory;
        private readonly SqlReplicationStatistics replicationStatistics;
        private readonly DbCommandBuilder commandBuilder;

        public RelationalDatabaseWriterSimulator( DocumentDatabase database, SqlReplicationConfig cfg, SqlReplicationStatistics replicationStatistics)
        {
            this.database = database;
            this.cfg = cfg;
            this.replicationStatistics = replicationStatistics;
            providerFactory = DbProviderFactories.GetFactory(cfg.FactoryName);
            commandBuilder = providerFactory.CreateCommandBuilder();
            if (SqlServerFactoryNames.Contains(cfg.FactoryName))
		    {
		        IsSqlServerFactoryType = true;
        }
        }
        private bool IsSqlServerFactoryType = false;
        private static string[] SqlServerFactoryNames =
        {
            "System.Data.SqlClient",
            "System.Data.SqlServerCe.4.0",
            "MySql.Data.MySqlClient",
            "System.Data.SqlServerCe.3.5"
        };

        public IEnumerable<string> SimulateExecuteCommandText(ConversionScriptResult scriptResult)
        {
            var identifiers = scriptResult.Data.SelectMany(x => x.Value).Select(x => x.DocumentId).Distinct().ToList();

            foreach (var sqlReplicationTable in cfg.SqlReplicationTables)
            {
                if(sqlReplicationTable.InsertOnlyMode)
                    continue;

                // first, delete all the rows that might already exist there
                foreach (string deleteQuery in GenerateDeleteItemsCommandText(sqlReplicationTable.TableName, sqlReplicationTable.DocumentKeyColumn, cfg.ParameterizeDeletesDisabled,
                    identifiers))
                {
                    yield return deleteQuery;
                }
            }

            foreach (var sqlReplicationTable in cfg.SqlReplicationTables)
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
                    .Append(itemToReplicate.DocumentId)
                    .Append(", ");

                foreach (var column in itemToReplicate.Columns)
                {
                    if (column.Key == pkName)
                        continue;
                    DbParameter param = new OdbcParameter();
                    RelationalDatabaseWriter.SetParamValue(param, column.Value, null);
                    sb.Append("'").Append(param.Value).Append("'").Append(", ");
                }
                sb.Length = sb.Length - 2;
                sb.Append(")");
                if (IsSqlServerFactoryType && cfg.ForceSqlServerQueryRecompile)
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

            database.WorkContext.CancellationToken.ThrowIfCancellationRequested();
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

                if (IsSqlServerFactoryType && cfg.ForceSqlServerQueryRecompile)
                {
                    sb.Append(" OPTION(RECOMPILE)");
                }

                sb.Append(";");
                yield return sb.ToString();
            }


        }

        private string GetTableNameString(string tableName)
        {
            if (cfg.QuoteTables)
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
