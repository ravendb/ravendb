using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Utils.Enumerators;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_25330 : RavenTestBase
    {
        private const int NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded = PulsedEnumerationState<object>.DefaultNumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded;

        public RavenDB_25330(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Encryption)]
        [InlineData(2 * NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 50)]
        [InlineData(4 * NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded + 10)]
        public async Task CanStreamQueryWithLoadAndPulsingReadTransaction(int numberOfTrainings)
        {
            string dbName = Encryption.SetupEncryptedDatabase(out var certificates, out var _);

            using (var store = GetDocumentStore(new Options
                   {
                       AdminCertificate = certificates.ServerCertificateForCommunication.Value,
                       ClientCertificate = certificates.ServerCertificateForCommunication.Value,
                       ModifyDatabaseName = s => dbName,
                       ModifyDatabaseRecord = record => {
                           record.Settings[RavenConfiguration.GetKey(x => x.Databases.PulseReadTransactionLimit)] = "0";
                           record.Encrypted = true;
                       },
                       Path = NewDataPath()
                   }))
            {
                var customerId = "customers/1";
                var packageId = "packages/1";

                using (var bulk = store.BulkInsert())
                {
                    await bulk.StoreAsync(new Customer { Name = "Test Customer" }, customerId);
                    await bulk.StoreAsync(new PurchasedPackage { Type = "Test Package" }, packageId);
                }

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < numberOfTrainings; i++)
                    {
                        await bulk.StoreAsync(new Training
                        {
                            OrganizationId = "organizations/1",
                            CustomerId = customerId,
                            PurchasedPackageId = packageId
                        });
                    }
                }

                var index = new Trainings_Index();
               
                await index.ExecuteAsync(store);

                await Indexes.WaitForIndexingAsync(store);

                var rql = $@"
                    from index '{index.IndexName}' as t
                    where t.OrganizationId == 'organizations/1'
                    load t.CustomerId as c, t.PurchasedPackageId as p
                    select {{
                        TrainingId: id(t),
                        CustomerName: c.Name,
                        PackageType: p.Type
                    }}";

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Advanced
                        .AsyncRawQuery<TrainingReport>(rql);

                    var enumerator = await session.Advanced.StreamAsync(query);

                    var count = 0;
                    while (await enumerator.MoveNextAsync())
                    {
                        count++;
                        var current = enumerator.Current.Document;

                        Assert.NotNull(current);
                        Assert.NotNull(current.CustomerName);
                        Assert.NotNull(current.PackageType);
                        Assert.Equal("Test Customer", current.CustomerName);
                        Assert.Equal("Test Package", current.PackageType);
                    }

                    Assert.Equal(numberOfTrainings, count);
                }
            }
        }

        private class Customer
        {
            public string Name { get; set; }
        }

        private class PurchasedPackage
        {
            public string Type { get; set; }
        }

        private class Training
        {
            public string OrganizationId { get; set; }
            public string CustomerId { get; set; }
            public string PurchasedPackageId { get; set; }
        }

        private class TrainingReport
        {
            public string TrainingId { get; set; }
            public string CustomerName { get; set; }
            public string PackageType { get; set; }
        }

        private class Trainings_Index : AbstractIndexCreationTask<Training>
        {
            public Trainings_Index()
            {
                Map = trainings => from training in trainings
                    select new
                    {
                        training.OrganizationId
                    };
            }
        }
    }
}
