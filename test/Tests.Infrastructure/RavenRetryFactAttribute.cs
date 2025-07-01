using System;
using xRetry;
using Xunit.Sdk;

namespace Tests.Infrastructure;

[TraitDiscoverer("Tests.Infrastructure.XunitExtensions.RavenTraitDiscoverer", "Tests.Infrastructure")]
public class RavenRetryFactAttribute : RetryFactAttribute, ITraitAttribute
{
    private string _skip;
    private readonly RavenTestCategory _category;
    public RavenRetryFactAttribute(RavenTestCategory category, int maxRetries = 3, int delayBetweenRetriesMs = 0, params Type[] skipOnExceptions)
    : base(maxRetries, delayBetweenRetriesMs, skipOnExceptions)
    {
        _category = category;
    }

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

    public override string Skip
    {
        get
        {
            return RavenFactAttribute.ShouldSkip(_skip, _category, licenseRequired: LicenseRequired, nightlyBuildRequired: NightlyBuildRequired, serviceRequirement: Requires);
        }

        set => _skip = value;
    }
}
