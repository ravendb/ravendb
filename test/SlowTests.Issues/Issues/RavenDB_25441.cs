using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_25441 : RavenTestBase
    {
        public RavenDB_25441(ITestOutputHelper output) : base(output)
        {
        }
        
        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task QueryWithLoadOnNullPropertyThatIsStoredInIndex(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var customerId = "customers/1";

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "Test Customer" }, customerId);
                    
                    session.Store(new Training
                    {
                        OrganizationId = "organizations/1",
                        CustomerId = customerId
                    });
                    
                    session.SaveChanges();
                }
                
                var index = new Trainings_Index_StorePackageIdField();
               
                await index.ExecuteAsync(store);

                await Indexes.WaitForIndexingAsync(store);

                var rql = $@"
                    from index '{index.IndexName}' as t
                    where t.OrganizationId == 'organizations/1'
                    load t.PurchasedPackageId as p
                    select {{
                        TrainingId: id(t),
                        PackageType: p.Type
                    }}";

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Advanced
                        .AsyncRawQuery<TrainingReport>(rql).ToListAsync();
                    
                    var current = (await query).Single();
                    
                    Assert.NotNull(current);
                    Assert.Null(current.PackageType);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task QueryWithLoadOnPropertyThatIsStoredInIndex(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var customerId = "customers/1";
                var packageId = "packages/1";
                
                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "Test Customer" }, customerId);
                    session.Store(new PurchasedPackage { Type = "Test Package" }, packageId);
                    
                    session.Store(new Training
                    {
                        OrganizationId = "organizations/1",
                        CustomerId = customerId,
                        PurchasedPackageId = packageId
                    });
                    
                    session.SaveChanges();
                }
                
                var index = new Trainings_Index_StorePackageIdField();
               
                await index.ExecuteAsync(store);

                await Indexes.WaitForIndexingAsync(store);

                var rql = $@"
                    from index '{index.IndexName}' as t
                    where t.OrganizationId == 'organizations/1'
                    load t.PurchasedPackageId as p
                    select {{
                        TrainingId: id(t),
                        PackageType: p.Type
                    }}";

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Advanced
                        .AsyncRawQuery<TrainingReport>(rql).ToListAsync();
                    
                    var current = (await query).Single();
                    
                    Assert.NotNull(current);
                    Assert.Equal("Test Package", current.PackageType);
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
        
        private class Trainings_Index_StorePackageIdField : AbstractIndexCreationTask<Training>
        {
            public Trainings_Index_StorePackageIdField()
            {
                Map = trainings => from training in trainings
                    select new
                    {
                        training.OrganizationId,
                        training.PurchasedPackageId
                    };
                
                Store(x => x.PurchasedPackageId, FieldStorage.Yes);
            }
        }
    }
}
