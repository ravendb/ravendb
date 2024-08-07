using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;
using Raven.Client.Documents.Operations.ETL.SQL;

namespace Raven.Client.Documents.Operations.ETL.Snowflake;

public sealed class SnowflakeEtlConfiguration : EtlConfiguration<SnowflakeConnectionString>
{
    private string _name;

    public SnowflakeEtlConfiguration()
    {
        SnowflakeTables = new List<SnowflakeEtlTable>();
    }

    public int? CommandTimeout { get; set; }

    public List<SnowflakeEtlTable> SnowflakeTables { get; set; }

    public override EtlType EtlType => EtlType.Snowflake;
        
        
    public override bool Validate(out List<string> errors, bool validateName = true, bool validateConnection = true)
    {
        base.Validate(out errors, validateName, validateConnection);

        if (SnowflakeTables.Count == 0)
            errors.Add($"{nameof(SnowflakeTables)} cannot be empty");

        return errors.Count == 0;
    }

    public override string GetDestination()
    {
        return _name ??= Connection?.GetDestination();
    }

    public override bool UsingEncryptedCommunicationChannel() => true;

    public override string GetDefaultTaskName()
    {
        return $"Snowflake ETL to {ConnectionStringName}";
    }

    public override DynamicJsonValue ToJson()
    {
        var result = base.ToJson();

        result[nameof(CommandTimeout)] = CommandTimeout;
        result[nameof(SnowflakeTables)] = new DynamicJsonArray(SnowflakeTables.Select(x => x.ToJson()));

        return result;
    }

    public override DynamicJsonValue ToAuditJson()
    {
        return ToJson();
    }
}

public class SnowflakeEtlTable
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

