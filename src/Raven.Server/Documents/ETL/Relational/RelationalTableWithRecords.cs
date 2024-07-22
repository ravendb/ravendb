using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.ETL.Snowflake;
using Raven.Client.Documents.Operations.ETL.SQL;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Relational;

public sealed class RelationalTableWithRecords
{
    
    public string TableName { get; set; }
    public string DocumentIdColumn { get; set; }
    public bool InsertOnlyMode { get; set; }

    private RelationalTableWithRecords()
    {
    }

    internal RelationalTableWithRecords(RelationalTableWithRecords tableWithRecords)
    {
        TableName = tableWithRecords.TableName;
        DocumentIdColumn = tableWithRecords.DocumentIdColumn;
        InsertOnlyMode = tableWithRecords.InsertOnlyMode;
    }

    public readonly List<ToRelationalItem> Inserts = [];

    public readonly List<ToRelationalItem> Deletes = [];

    public static RelationalTableWithRecords FromSnowflakeEtlTable(SnowflakeEtlTable snowflakeEtlTable)
    {
        return new RelationalTableWithRecords()
        {
            TableName = snowflakeEtlTable.TableName, DocumentIdColumn = snowflakeEtlTable.DocumentIdColumn, InsertOnlyMode = snowflakeEtlTable.InsertOnlyMode
        };
    }

    public static RelationalTableWithRecords FromSqlEtlTable(SqlEtlTable sqlEtlTable)
    {
        return new RelationalTableWithRecords()
        {
            TableName = sqlEtlTable.TableName, DocumentIdColumn = sqlEtlTable.DocumentIdColumn, InsertOnlyMode = sqlEtlTable.InsertOnlyMode
        };
    }
    
    
    public bool Equals(RelationalTableWithRecords other)
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
            [nameof(InsertOnlyMode)] = InsertOnlyMode //todo: shouldn't put inserts&deletes here? do i need it?
        };
    }
}
