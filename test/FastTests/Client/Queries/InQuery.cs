using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace FastTests.Client.Queries
{
    public class InQuery : RavenTestBase
    {
        public InQuery(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void QueryingUsingInShouldYieldDistinctResults(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Foo{Name = "Bar"},"Foos/1");
                    session.SaveChanges();
                    session.Query<Foo>().Single(foo => foo.Id.In("Foos/1", "Foos/1", "Foos/1", "Foos/1"));
                }

            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task CanQueryNullUsingInQuery(Options options)
        {
            using var store = GetDocumentStore(options);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Foo(){Name = null, Number = null});
                await session.StoreAsync(new Foo(){Name = string.Empty, Number = 1});
                await session.SaveChangesAsync();
                var resultNulls = await session.Advanced.AsyncRawQuery<Foo>("from Foos where Name in (0, null)").WaitForNonStaleResults().NoTracking().ToArrayAsync();
                Assert.Equal(1, resultNulls.Length);

                resultNulls = await session.Advanced.AsyncRawQuery<Foo>("from Foos where Name in (null, 0)").WaitForNonStaleResults().NoTracking().ToArrayAsync();
                Assert.Equal(1, resultNulls.Length);

                resultNulls = await session.Advanced.AsyncDocumentQuery<Foo>().WhereIn(x => x.Name, new[] {"0", null}).NoTracking().WaitForNonStaleResults().ToArrayAsync();
                Assert.Equal(1, resultNulls.Length);
                
                resultNulls = await session.Advanced.AsyncDocumentQuery<Foo>().WhereIn(x => x.Name, new[] {null, "0"}).NoTracking().WaitForNonStaleResults().ToArrayAsync();
                Assert.Equal(1, resultNulls.Length);
                
                resultNulls = await session.Advanced.AsyncDocumentQuery<Foo>().WhereIn(x => x.Number, new[] {0, (int?)null}).NoTracking().WaitForNonStaleResults().ToArrayAsync();
                Assert.Equal(1, resultNulls.Length);
                
                resultNulls = await session.Advanced.AsyncDocumentQuery<Foo>().WhereIn(x => x.Number, new[] {(int?)null, 0}).NoTracking().WaitForNonStaleResults().ToArrayAsync();
                Assert.Equal(1, resultNulls.Length);
            }
        }
        
        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanQueryDatesViaInQuery(Options options)
        {
            var random = new Random(1337);
            using var store = GetDocumentStore(options);
            var randomItems = Enumerable.Range(0, 10).Select(x => new Foo() {Date = new DateTime(random.NextInt64(DateTime.MinValue.Ticks, DateTime.MaxValue.Ticks))}).ToList();
            randomItems.Add(new Foo(){Date = null});
            using var session = store.OpenSession();
            randomItems.ForEach(x => session.Store(x));
            session.SaveChanges();

            var selectRandomToQuery = new []{randomItems[0], randomItems[3], randomItems[7]}.Select(x => x.Date).ToArray();
            var query = session.Advanced.DocumentQuery<Foo>().WhereIn(x => x.Date, selectRandomToQuery).WaitForNonStaleResults().ToList();
            Assert.Equal(3, query.Count);
            
            query = session.Advanced.DocumentQuery<Foo>().WhereIn(x => x.Date, selectRandomToQuery.Union(new []{(DateTime?)null})).WaitForNonStaleResults().ToList(); 
            Assert.Equal(4, query.Count);
        } 
        
        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void InQueryUseAnalyzerDuringQueryBuildingPhase(Options options)
        {
          
            using var store = GetDocumentStore(options);
            using var session = store.OpenSession();
            session.Store(new Foo(){Name = "MaCiEj"});
            session.Store(new Foo(){Name = "Gracjan"});
            session.Store(new Foo(){Name = null});
            session.SaveChanges();

            var query = session.Advanced.DocumentQuery<Foo>().WhereIn(x => x.Name, new []{"MACIEJ", null}).WaitForNonStaleResults().ToList();
            Assert.Equal(2, query.Count);
        } 
        
        private class Foo
        {
            public string Id { get; set; }
            public string Name { get; set; }
            
            public int? Number { get; set; }
            
            public DateTime? Date { get; set; }
        }

    }
}
