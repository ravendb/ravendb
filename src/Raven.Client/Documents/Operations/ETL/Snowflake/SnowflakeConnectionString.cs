using System.Collections.Generic;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.SQL;
using Sparrow.Json.Parsing;
namespace Raven.Client.Documents.Operations.ETL.Snowflake;

public sealed class SnowflakeConnectionString : ConnectionString
{
    public string ConnectionString { get; set; }

    public override ConnectionStringType Type => ConnectionStringType.Snowflake;

    protected override void ValidateImpl(ref List<string> errors)
    {
        if (string.IsNullOrEmpty(ConnectionString))
            errors.Add($"{nameof(ConnectionString)} cannot be empty");
    }

    public override bool IsEqual(ConnectionString connectionString)
    {
        if (connectionString is SnowflakeConnectionString snowflakeConnection)
        {
            return base.IsEqual(connectionString) && ConnectionString == snowflakeConnection.ConnectionString;
        }

        return false;
    }

    internal string GetDestination()
    {
        var accountId = SqlConnectionStringParser.GetConnectionStringValue(ConnectionString, ["Account"]);
        var database = SqlConnectionStringParser.GetConnectionStringValue(ConnectionString, ["Database", "Db"]);
        var schema = SqlConnectionStringParser.GetConnectionStringValue(ConnectionString, ["Schema"]);
        return $"{accountId}/{database}.{schema}";
    }

    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        json[nameof(ConnectionString)] = ConnectionString;

        return json;
    }

    public override DynamicJsonValue ToAuditJson()
    {
        return ToJson();
    }
}
