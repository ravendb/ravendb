using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Integrations.PostgreSQL;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Integrations.PostgreSQL;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Integrations.PostgreSQL;

public class RavenDB_16880 : RavenTestBase
{
    public RavenDB_16880(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task CanExportAndImportPostgreSqlIntegrationConfiguration()
    {
        using (var srcStore = GetDocumentStore())
        using (var dstStore = GetDocumentStore())

        {
            srcStore.Maintenance.Send(new ConfigurePostgreSqlOperation(new PostgreSqlConfiguration
            {
                Authentication = new PostgreSqlAuthenticationConfiguration()
                {
                    Users = new List<PostgreSqlUser>()
                        {
                            new PostgreSqlUser()
                            {
                                Username = "arek",
                                Password = "foo!@22"
                            }
                        }
                }
            }));

            var record = await srcStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(srcStore.Database));

            Assert.NotNull(record.Integrations);
            Assert.NotNull(record.Integrations.PostgreSql);
            Assert.Equal(1, record.Integrations.PostgreSql.Authentication.Users.Count);

            Assert.Contains("arek", record.Integrations.PostgreSql.Authentication.Users.First().Username);
            Assert.Contains("foo!@22", record.Integrations.PostgreSql.Authentication.Users.First().Password);

            var exportFile = GetTempFileName();

            var exportOperation = await srcStore.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportFile);
            await exportOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            var operation = await dstStore.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFile);
            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            record = await dstStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dstStore.Database));

            Assert.NotNull(record.Integrations);
            Assert.NotNull(record.Integrations.PostgreSql);
            Assert.Equal(1, record.Integrations.PostgreSql.Authentication.Users.Count);

            Assert.Contains("arek", record.Integrations.PostgreSql.Authentication.Users.First().Username);
            Assert.Contains("foo!@22", record.Integrations.PostgreSql.Authentication.Users.First().Password);
        }
    }
}
