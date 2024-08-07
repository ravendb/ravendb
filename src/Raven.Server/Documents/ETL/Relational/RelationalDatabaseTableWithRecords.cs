using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.ETL.Snowflake;
using Raven.Client.Documents.Operations.ETL.SQL;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Relational;

public sealed class RelationalDatabaseTableWithRecords
{
    
    public string TableName { get; set; }
    public string DocumentIdColumn { get; set; }
    public bool InsertOnlyMode { get; set; }

    private RelationalDatabaseTableWithRecords()
    {
    }

    internal RelationalDatabaseTableWithRecords(RelationalDatabaseTableWithRecords tableWithRecords)
    {
        TableName = tableWithRecords.TableName;
        DocumentIdColumn = tableWithRecords.DocumentIdColumn;
        InsertOnlyMode = tableWithRecords.InsertOnlyMode;
    }

    public readonly List<ToRelationalDatabaseItem> Inserts = [];

    public readonly List<ToRelationalDatabaseItem> Deletes = [];

    public static RelationalDatabaseTableWithRecords FromSnowflakeEtlTable(SnowflakeEtlTable snowflakeEtlTable)
    {
        return new RelationalDatabaseTableWithRecords()
        {
            TableName = snowflakeEtlTable.TableName, DocumentIdColumn = snowflakeEtlTable.DocumentIdColumn, InsertOnlyMode = snowflakeEtlTable.InsertOnlyMode
        };
    }

    public static RelationalDatabaseTableWithRecords FromSqlEtlTable(SqlEtlTable sqlEtlTable)
    {
        return new RelationalDatabaseTableWithRecords()
        {
            TableName = sqlEtlTable.TableName, DocumentIdColumn = sqlEtlTable.DocumentIdColumn, InsertOnlyMode = sqlEtlTable.InsertOnlyMode
        };
    }
    
    
    public bool Equals(RelationalDatabaseTableWithRecords other)
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
