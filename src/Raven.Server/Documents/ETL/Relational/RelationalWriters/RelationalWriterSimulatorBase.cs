
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;
using Raven.Server.Documents.ETL.Relational.Test;

namespace Raven.Server.Documents.ETL.Relational.RelationalWriters;

public abstract class RelationalWriterSimulatorBase<TRelationalEtlConfiguration, TRelationalCollectionString>
where TRelationalCollectionString: ConnectionString
where TRelationalEtlConfiguration: EtlConfiguration<TRelationalCollectionString>
{
    protected readonly TRelationalEtlConfiguration Configuration;
    protected readonly DbProviderFactory ProviderFactory;
    protected readonly DbCommandBuilder CommandBuilder;
    private readonly bool _parametrizeDeletes;

    public RelationalWriterSimulatorBase(TRelationalEtlConfiguration configuration,  bool parametrizeDeletes)
    {
        Configuration = configuration;
        _parametrizeDeletes = parametrizeDeletes;
        ProviderFactory = GetDbProviderFactory(configuration);
        CommandBuilder = GetInitializedCommandBuilder();
    }

    protected abstract DbProviderFactory GetDbProviderFactory(EtlConfiguration<TRelationalCollectionString> configuration);
    protected abstract DbCommandBuilder GetInitializedCommandBuilder();
    
    public IEnumerable<string> SimulateExecuteCommandText(RelationalTableWithRecords records, CancellationToken token)
    {
        if (records.InsertOnlyMode == false)
        {
            // first, delete all the rows that might already exist there
            foreach (var deleteQuery in GenerateDeleteItemsCommandText(records.TableName, records.DocumentIdColumn, _parametrizeDeletes,
                records.Deletes, token))
            {
                yield return deleteQuery;
            }
        }

        foreach (var insertQuery in GenerateInsertItemCommandText(records.TableName, records.DocumentIdColumn, records.Inserts, token))
        {
            yield return insertQuery;
        }
    }

    protected abstract DbParameter GetNewDbParameter();
    protected abstract string GetPostInsertIntoStartSyntax();
    protected abstract string GetPostInsertIntoEndSyntax();
    protected abstract string GetPostDeleteSyntax();
    

    protected abstract void SetParamValue(DbParameter colParam, RelationalColumn column, List<Func<DbParameter, string, bool>> stringParsers);
    private IEnumerable<string> GenerateInsertItemCommandText(string tableName, string pkName, List<ToRelationalItem> dataForTable, CancellationToken token)
    {
        foreach (var itemToReplicate in dataForTable)
        {
            token.ThrowIfCancellationRequested();
            
            var sb = new StringBuilder("INSERT INTO ")
                    .Append(GetTableNameString(tableName))
                    .Append(" (")
                    .Append(CommandBuilder.QuoteIdentifier(pkName))
                    .Append(", ");
            foreach (var column in itemToReplicate.Columns)
            {
                if (column.Id == pkName)
                    continue;
                sb.Append(CommandBuilder.QuoteIdentifier(column.Id)).Append(", ");
            }
            sb.Length = sb.Length - 2;

            sb.Append($") {GetPostInsertIntoStartSyntax()}")
                .Append("'")
                .Append(itemToReplicate.DocumentId)
                .Append("'")
                .Append(", ");

            foreach (var column in itemToReplicate.Columns)
            {
                if (column.Id == pkName)
                    continue;
                DbParameter param = GetNewDbParameter();
           
                SetParamValue(param, column, null);
                sb.Append(TableQuerySummary.GetParameterValue(param)).Append(", ");
            }
            
            sb.Length = sb.Length - 2;
            sb.Append(")");

            var endSyntax = GetPostInsertIntoEndSyntax();

            
            sb.Append(endSyntax);
            sb.Append(";");

            yield return sb.ToString();
        }
    }

    private IEnumerable<string> GenerateDeleteItemsCommandText(string tableName, string pkName, bool parameterize, List<ToRelationalItem> toSqlItems, CancellationToken token)
    {
        const int maxParams = 1000;

        token.ThrowIfCancellationRequested();

        for (int i = 0; i < toSqlItems.Count; i += maxParams)
        {
            var sb = new StringBuilder("DELETE FROM ")
                .Append(GetTableNameString(tableName))
                .Append(" WHERE ")
                .Append(CommandBuilder.QuoteIdentifier(pkName))
                .Append(" IN (");

            for (int j = i; j < Math.Min(i + maxParams, toSqlItems.Count); j++)
            {
                if (i != j)
                    sb.Append(", ");
                
                sb.Append("'").Append(SqlDatabaseWriter.SanitizeSqlValue(toSqlItems[j].DocumentId)).Append("'");
            }

            sb.Append(GetPostDeleteSyntax());


            sb.Append(";");
            yield return sb.ToString();
        }
    }

    protected abstract bool ShouldQuoteTables();

    private string GetTableNameString(string tableName)
    {
        return ShouldQuoteTables() ? string.Join(".", tableName.Split('.').Select(x => CommandBuilder.QuoteIdentifier(x)).ToArray()) : tableName;
    }
}

