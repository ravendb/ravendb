using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.Bugs.Iulian
{
    public class CanReadEntityWithUrlId : RavenTestBase
    {
        public CanReadEntityWithUrlId(ITestOutputHelper output) : base(output)
        {
        }

        private class Event
        {
            public string Id { get; set; }
            public string Tag { get; set; }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void Can_Load_entities_with_id_containing_url(Options options)
        {
            var id = @"mssage@msmq://local/Sample.AppService";

            DoNotReuseServer();
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    var e = new Event { Id = id, Tag = "tag" };
                    s.Store(e);
                    s.SaveChanges();
                }


                using (var s = store.OpenSession())
                {
                    var loaded = s.Query<Event>().Single(e => e.Id == id);

                    Assert.NotNull(loaded);
                    Assert.Equal("tag", loaded.Tag);
                }

                using (var s = store.OpenSession())
                {
                    var loaded = s.Load<Event>(id);

                    Assert.NotNull(loaded);
                    Assert.Equal("tag", loaded.Tag);
                }
            }
        }        
    }
}
