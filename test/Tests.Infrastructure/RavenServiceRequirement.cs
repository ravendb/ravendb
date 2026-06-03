using System;

namespace Tests.Infrastructure;

/// <summary>
/// Flags enum for external service requirements in RavenDB tests.
/// These flags specify which external databases and services are required for tests to run.
/// </summary>
[Flags]
public enum RavenServiceRequirement
{
    /// <summary>
    /// No external service requirements.
    /// </summary>
    None = 0,

    /// <summary>
    /// Test requires Microsoft SQL Server database.
    /// </summary>
    MsSql = 1 << 0,

    /// <summary>
    /// Test requires Oracle SQL database.
    /// </summary>
    OracleSql = 1 << 1,

    /// <summary>
    /// Test requires PostgreSQL database.
    /// </summary>
    NpgSql = 1 << 2,

    /// <summary>
    /// Test requires MongoDB database.
    /// </summary>
    MongoDB = 1 << 3,

    /// <summary>
    /// Test requires ElasticSearch.
    /// </summary>
    ElasticSearch = 1 << 4,

    /// <summary>
    /// Test requires Azure Queue Storage.
    /// </summary>
    AzureQueueStorage = 1 << 5,
    
    /// <summary>
    /// Test requires Snowflake.
    /// </summary>
    Snowflake = 1 << 6,
    
    /// <summary>
    /// Test requires Amazon SQS.
    /// </summary>
    AmazonSqs = 1 << 7,

    /// <summary>
    /// Test requires MySQL.
    /// </summary>
    MySql = 1 << 8,

    /// <summary>
    /// Test requires AWS services.
    /// </summary>
    Aws = 1 << 9,

    /// <summary>
    /// Test requires Azure services.
    /// </summary>
    Azure = 1 << 10,

    /// <summary>
    /// Test requires Azure Service Bus.
    /// </summary>
    AzureServiceBus = 1 << 11,

    /// <summary>
    /// Test requires SQL Server with CDC support (Enterprise, Developer, or Standard edition — not Express).
    /// </summary>
    MsSqlCdc = 1 << 12
}
