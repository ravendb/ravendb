using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Oregon : RavenTestBase
    {
        public Oregon(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanQueryForOregon(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Query<User>()
                        .Where(x => x.LastName == "OR")
                        .ToList();
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, DatabaseMode = RavenDatabaseMode.All)]
        public void Fails_Lucene(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Dummy { Boolean = false, Object = null });
                    s.Store(new Dummy { Boolean = true, Object = null });
                    s.Store(new Dummy { Boolean = false, Object = new Dummy() });
                    s.Store(new Dummy { Boolean = true, Object = new Dummy() });
                    s.SaveChanges();
                }
                using (var s = store.OpenSession())
                {
                    var objects = s.Query<Dummy>()        
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Boolean || x.Object != null)
                        .ToArray();

                    Assert.Equal(3, objects.Length); // objects.Length is 2
                }
            }
        }

        
        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
        public void Fails_Corax(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Dummy { Boolean = false, Object = null });
                    s.Store(new Dummy { Boolean = true, Object = null });
                    s.Store(new Dummy { Boolean = false, Object = new Dummy() });
                    s.Store(new Dummy { Boolean = true, Object = new Dummy() });
                    s.SaveChanges();
                }
                using (var s = store.OpenSession())
                {
                    new DummyCoraxIndex().Execute(store);
                    var objects = s.Query<Dummy, DummyCoraxIndex>()        
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Boolean || x.Object != null)
                        .ToArray();
                    WaitForUserToContinueTheTest(store);
                    Assert.Equal(3, objects.Length); // objects.Length is 2
                }
            }
        }

        
        private class Dummy
        {
            public bool Boolean { get; set; }
            public Dummy Object { get; set; }
        }

        private class DummyCoraxIndex : AbstractIndexCreationTask<Dummy>
        {
            public DummyCoraxIndex()
            {
                Map = dummies => dummies.Select(n => new {Boolean = n.Boolean, Object = n.Object == null ? null : n.Object.ToString()});
            }
        }
    }
}
