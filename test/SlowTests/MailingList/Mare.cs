using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Mare : RavenTestBase
    {
        public Mare(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanUnderstandEqualsMethod()
        {
            using (var store = GetDocumentStore())
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
