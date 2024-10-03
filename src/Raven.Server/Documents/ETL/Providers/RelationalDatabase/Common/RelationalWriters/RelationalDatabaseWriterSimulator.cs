using System.Collections.Generic;
using System.Threading;

namespace Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common.RelationalWriters;

public class RelationalDatabaseWriterSimulator(IRelationalDatabaseWriter writer, bool parametrizeDeletes = false)
{
    public IEnumerable<string> SimulateExecuteCommandText(RelationalDatabaseTableWithRecords records, CancellationToken token)
    {
        if (records.InsertOnlyMode == false)
        {
            // first, delete all the rows that might already exist there
            foreach (var deleteQuery in GenerateDeleteItemsCommandText(records.TableName, records.DocumentIdColumn, parametrizeDeletes,
                         records.Deletes, token))
                yield return deleteQuery;
        }

        foreach (var insertQuery in GenerateInsertItemCommandText(records.TableName, records.DocumentIdColumn, records.Inserts, token))
            yield return insertQuery;
    }
    
    private IEnumerable<string> GenerateInsertItemCommandText(string tableName, string pkName, List<RelationalDatabaseItem> dataForTable, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        
        foreach (var itemToReplicate in dataForTable)
        {
            var command = writer.GetInsertCommand(tableName, pkName, itemToReplicate);
            var commandText = command.CommandText;
            command.Dispose();
            yield return commandText;
        }
    }

    private IEnumerable<string> GenerateDeleteItemsCommandText(string tableName, string pkName, bool parameterize, List<RelationalDatabaseItem> toSqlItems, CancellationToken token)
    {
        const int maxParams = 1000;

        token.ThrowIfCancellationRequested();

        for (int i = 0; i < toSqlItems.Count; i += maxParams)
        {
            using (var command = writer.GetDeleteCommand(tableName, pkName, toSqlItems, i, parameterize, maxParams, out int countOfDeletes))
            {
                var commandText = command.CommandText;
                yield return commandText;    
            }
        }
    }
}
