using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.MailingList
{
    public class Mare : RavenTestBase
    {
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
