using System.Linq;
using FastTests;
using Newtonsoft.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class RenamedProperty : RavenTestBase
    {
        public RenamedProperty(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void OrderByWithAttributeShouldStillWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                const int count = 1000;

                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < count; i++)
                    {
                        var model = new MyClass
                        {
                            ThisWontWork = i,
                            ThisWillWork = i
                        };
                        session.Store(model);
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var orderedWithoutAttribute = session.Query<MyClass>().Customize(x => x.WaitForNonStaleResults()).OrderBy(x => x.ThisWillWork).Take(count).ToList();
                    var orderedWithAttribute = session.Query<MyClass>().Customize(x => x.WaitForNonStaleResults()).OrderByDescending(x => x.ThisWontWork).Take(count).ToList();

                    Assert.Equal(count, orderedWithoutAttribute.Count);
                    Assert.Equal(count, orderedWithAttribute.Count);

                    for (var i = 1; i <= count; i++)
                    {
                        Assert.Equal(orderedWithoutAttribute[i - 1].ThisWontWork, orderedWithAttribute[count - i].ThisWontWork);
                    }
                }
            }
        }

        private class MyClass
        {
            [JsonProperty("whoops")]
            public long ThisWontWork { get; set; }

            public long ThisWillWork { get; set; }
        }
    }
}
