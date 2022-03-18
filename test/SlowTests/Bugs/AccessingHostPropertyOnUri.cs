using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class AccessingHostPropertyOnUri : RavenTestBase
    {
        public AccessingHostPropertyOnUri(ITestOutputHelper output) : base(output)
        {
        }

        private class WebItem
        {
            public Uri Url { get; set; }
        }

        private class WebItems
        {
            public string Owner { get; set; }

            public IList<WebItem> Items = new List<WebItem>();
        }

        private class WebActivityIndex : AbstractIndexCreationTask<WebItems>
        {
            public WebActivityIndex()
            {
                Map = activities => from activity in activities
                                    let mapping = from state in activity.Items
                                                  select new
                                                  {
                                                      Owner = activity.Owner,
                                                      Host = new Uri(state.Url.ToString()).Host,
                                                      Url = state.Url,
                                                  }
                                    from state in mapping
                                    group state by new
                                    {
                                        Owner = state.Owner,
                                        Host = state.Host,
                                        Url = state.Url,
                                    } into g
                                    select new
                                    {
                                        Owner = g.Key.Owner,
                                        Host = g.Key.Host,
                                        Url = g.Key.Url,
                                    };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        [Fact]
        public async Task ShouldNotConvertUriToStringWhenIndexing()
        {
            using (var store = GetDocumentStore())
            {
                new WebActivityIndex().Execute(store);

                var activities1 = new WebItems()
                {
                    Owner = "owner",
                    Items = new List<WebItem>
                    {
                        new WebItem{ Url = new Uri (@"http://domain1.com") },
                        new WebItem{ Url = new Uri (@"http://domain2.com") },
                        new WebItem{ Url = new Uri (@"http://domain3.com") },
                    }
                };

                var activities2 = new WebItems()
                {
                    Owner = "owner",
                    Items = new List<WebItem>
                    {
                        new WebItem{ Url = new Uri (@"http://domain2.com") },
                        new WebItem{ Url = new Uri (@"http://domain3.com") },
                        new WebItem{ Url = new Uri (@"http://domain4.com") },
                    }
                };

                using (var session = store.OpenSession())
                {
                    session.Store(activities1, "test1");
                    session.Store(activities2, "test2");
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);
                var errorsCount = db.IndexStore.GetIndexes().Sum(index => index.GetErrorCount());

                Assert.Equal(errorsCount, 0);
            }
        }
    }
}
