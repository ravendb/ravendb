using System.Collections.Generic;
using System.Data.Common;

namespace Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common.RelationalWriters;

public interface IRelationalDatabaseWriter
{
    DbCommand GetInsertCommand(string tableName, string pkName, RelationalDatabaseItem itemToReplicate);
    DbCommand GetDeleteCommand(string tableName, string pkName, List<RelationalDatabaseItem> toDeleteSqlItems, int currentToDeleteIndex, bool parametrizeDeletes , int maxParams, out int countOfDeletes);
}
