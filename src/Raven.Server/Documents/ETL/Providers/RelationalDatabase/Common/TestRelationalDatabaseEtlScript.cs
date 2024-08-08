using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL.Test;

namespace Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common;

public sealed class TestRelationalDatabaseEtlScript<TRelationalConnectionString, TRelationalEtlConfiguration> : TestEtlScript<TRelationalEtlConfiguration, TRelationalConnectionString>
    where TRelationalConnectionString: ConnectionString
    where TRelationalEtlConfiguration: EtlConfiguration<TRelationalConnectionString>
{
    public bool PerformRolledBackTransaction;

    public TRelationalConnectionString Connection;
}
