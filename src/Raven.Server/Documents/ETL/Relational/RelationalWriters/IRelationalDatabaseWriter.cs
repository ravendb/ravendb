using System.Collections.Generic;
using System.Data.Common;

namespace Raven.Server.Documents.ETL.Relational.RelationalWriters;

public interface IRelationalDatabaseWriter
{
    DbCommand GetInsertCommand(string tableName, string pkName, ToRelationalDatabaseItem itemToReplicate);
    DbCommand GetDeleteCommand(string tableName, string pkName, List<ToRelationalDatabaseItem> toDeleteSqlItems, int currentToDeleteIndex, bool parametrizeDeletes , int maxParams, out int countOfDeletes);
}
