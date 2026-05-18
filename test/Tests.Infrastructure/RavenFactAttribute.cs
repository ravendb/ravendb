using System;
using System.Collections.Generic;
using Tests.Infrastructure.ConnectionString;
using Tests.Infrastructure.XunitExtensions;
using Xunit;
using Xunit.v3;

namespace Tests.Infrastructure;

public class RavenFactAttribute : FactAttribute, ITraitAttribute, Xunit.v3.IFactAttribute
{
    string Xunit.v3.IFactAttribute.Skip => this.Skip;

    public readonly RavenTestCategory Category;
    private string _skip;

    public RavenFactAttribute(RavenTestCategory category)
    {
        Category = category;
    }

    public IReadOnlyCollection<KeyValuePair<string, string>> GetTraits() =>
        RavenTraitHelper.GetTraitsFor(Category);

    public bool LicenseRequired { get; set; }

    public bool NightlyBuildRequired { get; set; }

    public RavenServiceRequirement Requires { get; set; } = RavenServiceRequirement.None;

    // Legacy properties for backward compatibility
    public bool MsSqlRequired 
    { 
        get => Requires.HasFlag(RavenServiceRequirement.MsSql);
        set => Requires = value ? Requires | RavenServiceRequirement.MsSql : Requires & ~RavenServiceRequirement.MsSql;
    }

    public bool ElasticSearchRequired 
    { 
        get => Requires.HasFlag(RavenServiceRequirement.ElasticSearch);
        set => Requires = value ? Requires | RavenServiceRequirement.ElasticSearch : Requires & ~RavenServiceRequirement.ElasticSearch;
    }

    public bool AzureQueueStorageRequired 
    { 
        get => Requires.HasFlag(RavenServiceRequirement.AzureQueueStorage);
        set => Requires = value ? Requires | RavenServiceRequirement.AzureQueueStorage : Requires & ~RavenServiceRequirement.AzureQueueStorage;
    }

    public bool OracleSqlRequired 
    { 
        get => Requires.HasFlag(RavenServiceRequirement.OracleSql);
        set => Requires = value ? Requires | RavenServiceRequirement.OracleSql : Requires & ~RavenServiceRequirement.OracleSql;
    }

    public bool NpgSqlRequired 
    { 
        get => Requires.HasFlag(RavenServiceRequirement.NpgSql);
        set => Requires = value ? Requires | RavenServiceRequirement.NpgSql : Requires & ~RavenServiceRequirement.NpgSql;
    }

    public bool MongoDBRequired 
    { 
        get => Requires.HasFlag(RavenServiceRequirement.MongoDB);
        set => Requires = value ? Requires | RavenServiceRequirement.MongoDB : Requires & ~RavenServiceRequirement.MongoDB;
    }
    
    public bool SnowflakeRequired  
    { 
        get => Requires.HasFlag(RavenServiceRequirement.Snowflake);
        set => Requires = value ? Requires | RavenServiceRequirement.Snowflake : Requires & ~RavenServiceRequirement.Snowflake;
    }
    
    public bool AmazonSqsRequired
    { 
        get => Requires.HasFlag(RavenServiceRequirement.AmazonSqs);
        set => Requires = value ? Requires | RavenServiceRequirement.AmazonSqs : Requires & ~RavenServiceRequirement.AmazonSqs;
    }

    public bool AwsRequired
    {
        get => Requires.HasFlag(RavenServiceRequirement.Aws);
        set => Requires = value ? Requires | RavenServiceRequirement.Aws : Requires & ~RavenServiceRequirement.Aws;
    }

    public bool AzureRequired
    {
        get => Requires.HasFlag(RavenServiceRequirement.Azure);
        set => Requires = value ? Requires | RavenServiceRequirement.Azure : Requires & ~RavenServiceRequirement.Azure;
    }

    public new string Skip
    {
        get => ShouldSkip(_skip, Category, licenseRequired: LicenseRequired, nightlyBuildRequired: NightlyBuildRequired, serviceRequirement: Requires);
        set => _skip = value;
    }

    internal static string ShouldSkip(string skip, RavenTestCategory category, bool licenseRequired, bool nightlyBuildRequired, RavenServiceRequirement serviceRequirement)
    {
        var s = ShouldSkip(skip, category, licenseRequired: licenseRequired, nightlyBuildRequired: nightlyBuildRequired);
        if (s != null)
            return s;

        if (serviceRequirement.HasFlag(RavenServiceRequirement.MsSql) && ShouldSkipMsSql(out skip))
            return skip;

        if (serviceRequirement.HasFlag(RavenServiceRequirement.OracleSql) && ShouldSkipOracleSql(out skip))
            return skip;

        if (serviceRequirement.HasFlag(RavenServiceRequirement.NpgSql) && ShouldSkipNpgSql(out skip))
            return skip;

        if (serviceRequirement.HasFlag(RavenServiceRequirement.MongoDB) && ShouldSkipMongoDB(out skip))
            return skip;

        if (serviceRequirement.HasFlag(RavenServiceRequirement.ElasticSearch) && ShouldSkipElasticSearch(out skip))
            return skip;

        if (serviceRequirement.HasFlag(RavenServiceRequirement.AzureQueueStorage) && ShouldSkipAzureQueueStorage(out skip))
            return skip;
        
        if (serviceRequirement.HasFlag(RavenServiceRequirement.Snowflake) && ShouldSkipSnowflake(out skip))
            return skip;
        
        if (serviceRequirement.HasFlag(RavenServiceRequirement.AmazonSqs) && ShouldSkipAmazonSqs(out skip))
            return skip;

        if (serviceRequirement.HasFlag(RavenServiceRequirement.MySql) && ShouldSkipMySql(out skip))
            return skip;

        if (serviceRequirement.HasFlag(RavenServiceRequirement.Aws) && ShouldSkipAws(out skip))
            return skip;

        if (serviceRequirement.HasFlag(RavenServiceRequirement.Azure) && ShouldSkipAzure(out skip))
            return skip;

        return null;
    }

    private static string ShouldSkip(string skip, RavenTestCategory category, bool licenseRequired, bool nightlyBuildRequired)
    {
        if (skip != null)
            return skip;

        if (RavenDataAttributeBase.Is32Bit)
        {
            if (category.HasFlag(RavenTestCategory.Sharding))
                return RavenDataAttributeBase.ShardingSkipMessage;
        }

        if (licenseRequired && ShouldSkipLicense(out skip))
            return skip;

        if (nightlyBuildRequired && NightlyBuildFactAttribute.ShouldSkip(out skip))
            return skip;

        return null;
    }

    private static bool ShouldSkipService(Func<bool> canConnect, string serviceName, out string skipMessage)
    {
        if (RavenTestHelper.EnvironmentVariables.SkipIntegrationTests)
        {
            skipMessage = RavenTestHelper.SkipIntegrationMessage;
            return true;
        }

        if (RavenTestHelper.EnvironmentVariables.IsRunningOnCI)
        {
            skipMessage = null;
            return false;
        }

        if (canConnect())
        {
            skipMessage = null;
            return false;
        }

        skipMessage = $"Test requires {serviceName}";
        return true;
    }

    private static bool ShouldSkipMsSql(out string skipMessage) =>
        ShouldSkipService(() => MsSqlConnectionString.Instance.CanConnect, "MsSQL database", out skipMessage);

    private static bool ShouldSkipOracleSql(out string skipMessage) =>
        ShouldSkipService(() => OracleConnectionString.Instance.CanConnect, "Oracle database", out skipMessage);

    private static bool ShouldSkipNpgSql(out string skipMessage) =>
        ShouldSkipService(() => NpgSqlConnectionString.Instance.CanConnect, "NpgSQL database", out skipMessage);

    private static bool ShouldSkipMongoDB(out string skipMessage) =>
        ShouldSkipService(() => MongoDBConnectionString.Instance.CanConnect, "MongoDB", out skipMessage);

    private static bool ShouldSkipElasticSearch(out string skipMessage) =>
        ShouldSkipService(() => ElasticSearchTestNodes.Instance.CanConnect, "ElasticSearch instance", out skipMessage);

    private static bool ShouldSkipMySql(out string skipMessage) =>
        ShouldSkipService(() => MySqlConnectionString.Instance.CanConnect, "MySQL database", out skipMessage);

    private static bool ShouldSkipAzureQueueStorage(out string skipMessage)
    {
        return AzureQueueStorageHelper.ShouldSkip(out skipMessage);
    }
    
    private static bool ShouldSkipSnowflake(out string skipMessage)
    {
        return SnowflakeHelper.ShouldSkip(out skipMessage);
    }
    
    private static bool ShouldSkipAmazonSqs(out string skipMessage)
    {
        return AmazonSqsHelper.ShouldSkip(out skipMessage);
    }

    private static bool ShouldSkipAws(out string skipMessage)
    {
        return AmazonS3RetryFactAttribute.ShouldSkip(out skipMessage);
    }

    private static bool ShouldSkipAzure(out string skipMessage)
    {
        return AzureRetryFactAttribute.ShouldSkip(out skipMessage);
    }

    internal static bool ShouldSkipLicense(out string skipMessage)
    {
        if (RavenTestHelper.EnvironmentVariables.HasLicense)
        {
            skipMessage = null;
            return false;
        }

        skipMessage = $"Requires License to be set via '{RavenTestHelper.EnvironmentVariables.LicenseKey}' environment variable.";
        return true;
    }
}
