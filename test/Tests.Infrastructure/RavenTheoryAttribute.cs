using Xunit;
using Xunit.Sdk;

namespace Tests.Infrastructure;

[TraitDiscoverer("Tests.Infrastructure.XunitExtensions.RavenTraitDiscoverer", "Tests.Infrastructure")]
public class RavenTheoryAttribute : TheoryAttribute, ITraitAttribute
{
    private string _skip;
    public readonly RavenTestCategory Category;

    public RavenTheoryAttribute(RavenTestCategory category)
    {
        Category = category;
    }

    public bool LicenseRequired { get; set; }

    public bool NightlyBuildRequired { get; set; }

    public bool S3Required { get; set; }

    public bool AzureRequired { get; set; }

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

    public override string Skip
    {
        get
        {
            return ShouldSkip(_skip, Category, licenseRequired: LicenseRequired, nightlyBuildRequired: NightlyBuildRequired, serviceRequirement: Requires, s3Required: S3Required, azureRequired: AzureRequired);
        }

        set => _skip = value;
    }

    internal static string ShouldSkip(string skip, RavenTestCategory category, bool licenseRequired, bool nightlyBuildRequired, RavenServiceRequirement serviceRequirement, bool s3Required, bool azureRequired)
    {
        var s = RavenFactAttribute.ShouldSkip(skip, category, licenseRequired: licenseRequired, nightlyBuildRequired: nightlyBuildRequired, serviceRequirement: serviceRequirement);
        if (s != null)
            return s;

        if (s3Required && AmazonS3RetryTheoryAttribute.ShouldSkip(out skip))
            return skip;

        if (azureRequired && AzureRetryTheoryAttribute.ShouldSkip(out skip))
            return skip;

        return null;
    }

}
