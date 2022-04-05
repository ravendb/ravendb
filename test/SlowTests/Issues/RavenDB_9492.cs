using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9492: RavenTestBase
    {
        public RavenDB_9492(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void BetweenQueryOnIds(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Bunny { Name = "what" }, "bunny/1");
                    session.Store(new Bunny { Name = "is" }, "bunny/2");
                    session.Store(new Bunny { Name = "your" }, "bunny/3");
                    session.SaveChanges();

                    // passing
                    var results = session.Advanced.RawQuery<Bunny>("from Bunnies as b where id(b) between 'bunny/1' and 'bunny/2'").ToList();

                    Assert.Equal(2, results.Count);
                    var indexNames = store.Maintenance.Send(new GetIndexNamesOperation(0, 10));
                    Assert.Equal(1, indexNames.Length);
                 
                    // fails
                    results = session.Advanced.DocumentQuery<Bunny>().WhereBetween(x => x.Id, "bunny/1", "bunny/2").WaitForNonStaleResults().ToList();

                    Assert.Equal(2, results.Count);
                }
            }
        }
        
        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanQueryIdWithNegate(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Bunny{Name="what"}, "bunny/1");
                    session.Store(new Bunny { Name = "is" }, "bunny/2");
                    session.Store(new Bunny { Name = "your" }, "bunny/3");
                    session.SaveChanges();
                    var query = session.Query<Bunny>().Where(u => u.Id != "bunny/1").ToList();
                    //We want to make sure this kind of query generates an index
                    var indexNames = store.Maintenance.Send(new GetIndexNamesOperation(0, 10));
                    Assert.Equal(1, indexNames.Length);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void InQueryOnIdsShouldRunOnCollection(Options options)
        {
            var bunnies = new List<string>
            {
                "bunny/1",
                "bunny/2"
            };
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Bunny { Name = "what" }, "bunny/1");
                    session.Store(new Bunny { Name = "is" }, "bunny/2");
                    session.Store(new Bunny { Name = "your" }, "bunny/3");
                    session.SaveChanges();
                    var query = session.Query<Bunny>().Where(u => u.Id.In(bunnies)).ToList();
                    Assert.Equal(2, query.Count);
                    //We want to make sure no index was created for such query
                    var indexNames = store.Maintenance.Send(new GetIndexNamesOperation(0, 10));
                    Assert.Empty(indexNames);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void NotInQueryOnIdsShouldntRunOnCollection(Options options)
        {
            var bunnies = new List<string>
            {
                "bunny/1",
                "bunny/2"
            };
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Bunny { Name = "what" }, "bunny/1");
                    session.Store(new Bunny { Name = "is" }, "bunny/2");
                    session.Store(new Bunny { Name = "your" }, "bunny/3");
                    session.SaveChanges();
                }

                {
                 //   WaitForUserToContinueTheTest(store);
                    using var session = store.OpenSession();
                    var query = session.Query<Bunny>().Where(u => !u.Id.In(bunnies)).Customize(x=>x.WaitForNonStaleResults()).ToList();
                    WaitForUserToContinueTheTest(store);
                    Assert.Equal(1, query.Count);
                    //We want to make sure this kind of query generates an index
                    var indexNames = store.Maintenance.Send(new GetIndexNamesOperation(0, 10));
                    Assert.Equal(1, indexNames.Length);
                }
            }
        }
        private class Bunny
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
