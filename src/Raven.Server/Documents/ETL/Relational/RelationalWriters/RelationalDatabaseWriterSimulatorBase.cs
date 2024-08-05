using System.Collections.Generic;
using System.Threading;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL.Relational.Metrics;

namespace Raven.Server.Documents.ETL.Relational.RelationalWriters;

public abstract class RelationalDatabaseWriterSimulatorBase<TRelationalConnectionString, TRelationalEtlConfiguration>: RelationalDatabaseWriterBase<TRelationalConnectionString, TRelationalEtlConfiguration>
where TRelationalConnectionString: ConnectionString
where TRelationalEtlConfiguration: EtlConfiguration<TRelationalConnectionString>
{
    protected RelationalDatabaseWriterSimulatorBase(TRelationalEtlConfiguration configuration, DocumentDatabase database,
        RelationalDatabaseEtlMetricsCountersManager etlMetricsCountersManager, EtlProcessStatistics etlProcessStatistics) : base(database, configuration,
        etlMetricsCountersManager, etlProcessStatistics)
    {
    }

    public IEnumerable<string> SimulateExecuteCommandText(RelationalDatabaseTableWithRecords records, CancellationToken token)
    {
        if (records.InsertOnlyMode == false)
        {
            // first, delete all the rows that might already exist there
            foreach (var deleteQuery in GenerateDeleteItemsCommandText(records.TableName, records.DocumentIdColumn, ParametrizeDeletes,
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
    
    private IEnumerable<string> GenerateInsertItemCommandText(string tableName, string pkName, List<ToRelationalDatabaseItem> dataForTable, CancellationToken token)
    {
        foreach (var itemToReplicate in dataForTable)
        {
            var command = CreateCommand();
            FillInsertCommand(command, tableName, pkName, itemToReplicate);
            yield return command.CommandText;
        }
    }

    private IEnumerable<string> GenerateDeleteItemsCommandText(string tableName, string pkName, bool parameterize, List<ToRelationalDatabaseItem> toSqlItems, CancellationToken token)
    {
        const int maxParams = 1000;

        token.ThrowIfCancellationRequested();

        for (int i = 0; i < toSqlItems.Count; i += maxParams)
        {
            var cmd = CreateCommand();
            FillDeleteCommand(cmd, tableName, pkName, maxParams, toSqlItems, i, parameterize);
            yield return cmd.CommandText;
        }
    }
}

