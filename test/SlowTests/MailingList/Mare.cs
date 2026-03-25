using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.MailingList
{
    public class Mare : RavenTestBase
    {
        public Mare(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanUnderstandEqualsMethod(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    s.Query<User>().Where(x => x.Age.Equals(10)).ToList();
                }
            }
        }

        private class User
        {
            public int Age { get; set; }
        }
    }
}
