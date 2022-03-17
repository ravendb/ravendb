using FastTests;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RDBC_319 : RavenTestBase
    {
        public RDBC_319(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanStreamQueryResults()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 200; i++)
                    {
                        session.Store(new User());
                    }
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                int count = 0;

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<User>("from Users");

                    var reader = session.Advanced.Stream(query, out var stats);

                    while (reader.MoveNext())
                    {
                        count++;
                        Assert.IsType<User>(reader.Current.Document);
                    }

                    Assert.Equal(200, stats.TotalResults);
                }

                Assert.Equal(200, count);
                count = 0;

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<User>("from Users");

                    var reader = session.Advanced.Stream(query, out var stats);
                    while (reader.MoveNext())
                    {
                        count++;
                        Assert.IsType<User>(reader.Current.Document);

                    }

                    Assert.Equal(200, stats.TotalResults);

                }

                Assert.Equal(200, count);
            }
        }
    }
}
