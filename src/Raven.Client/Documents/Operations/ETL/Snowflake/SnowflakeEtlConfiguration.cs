using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Extensions;
using Sparrow.Json.Parsing;

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
        
        
    public override bool Validate(out List<string> errors, bool validateName = true, bool validateConnection = true, bool validateIdentifier = true, EtlConfiguration<SnowflakeConnectionString> existingConfiguration = null)
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

    internal override EtlConfigurationCompareDifferences Compare(EtlConfiguration<SnowflakeConnectionString> config, Dictionary<string, SnowflakeConnectionString> connectionStrings, List<(string TransformationName, EtlConfigurationCompareDifferences Difference)> transformationDiffs = null)
    {
        var diff = base.Compare(config, connectionStrings, transformationDiffs);

        if (config is SnowflakeEtlConfiguration snowflakeConfig)
        {
            if (snowflakeConfig.CommandTimeout != CommandTimeout ||
                EnumerableExtension.ContentEqualsIgnoringOrder(snowflakeConfig.SnowflakeTables, SnowflakeTables) == false)
                diff |= EtlConfigurationCompareDifferences.ConfigurationOptions;
        }

        return diff;
    }

    public override string GetDefaultTaskName()
    {
        return $"Snowflake ETL to {ConnectionStringName}";
    }

    public override DynamicJsonValue ToJson()
    {
        var result = base.ToJson();

        result[nameof(CommandTimeout)] = CommandTimeout;
        result[nameof(SnowflakeTables)] = SnowflakeTables != null ? new DynamicJsonArray(SnowflakeTables.Select(x => x.ToJson())) : null;

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

    private bool Equals(SnowflakeEtlTable other)
    {
        return string.Equals(TableName, other.TableName) && string.Equals(DocumentIdColumn, other.DocumentIdColumn, StringComparison.OrdinalIgnoreCase) &&
               InsertOnlyMode == other.InsertOnlyMode;
    }

    public override bool Equals(object obj)
    {
        return ReferenceEquals(this, obj) || obj is SnowflakeEtlTable other && Equals(other);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(TableName, DocumentIdColumn?.ToLowerInvariant(), InsertOnlyMode);
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

