using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Client;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20979 : RavenTestBase
{
    public RavenDB_20979(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task CanUsePulsedEnumeratorInDictionaryTrainingPhase(Options parameters)
    {
        var result = await Encryption.EncryptedServerAsync();

        using var store = GetDocumentStore(new Options
        {
            Encrypted = true,
            AdminCertificate = result.Certificates.ServerCertificate.Value,
            ClientCertificate = result.Certificates.ServerCertificate.Value,
            ModifyDatabaseName = s => result.DatabaseName,
            ModifyDatabaseRecord = record =>
            {
                parameters.ModifyDatabaseRecord(record);
                record.Settings[RavenConfiguration.GetKey(x => x.Databases.PulseReadTransactionLimit)] = "0";
                record.Encrypted = true;
            }
        });

        using (var bulkInsert = store.BulkInsert())
        {
            for (int i = 0; i < 1024*5; ++i)
            {
                bulkInsert.Store(new Query.Order() {Employee = i.ToString()});
            }
        }

        using (var session = store.OpenSession())
        {
            var autoIndex = session.Query<Query.Order>()
                .Customize(i => i.WaitForNonStaleResults(waitTimeout: TimeSpan.FromMinutes(1)))
                .Statistics(out var statistics)
                .Count(i => i.Employee.StartsWith("1"));
            Assert.True(autoIndex > 0);
            
            var indexErrors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] {statistics.IndexName}));
            Assert.Empty(indexErrors.First(i=> i.Name == statistics.IndexName).Errors);
        }
    }
}
