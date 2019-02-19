using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB934 : RavenTestBase
    {
        private class User
        {
        }

        [Fact]
        public void LowLevelExportsByDoc()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 1500; i++)
                    {
                        session.Store(new User());
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var count = 0;
                    using (var streamDocs = session.Advanced.Stream<object>(startsWith: ""))
                    {
                        while (streamDocs.MoveNext())
                        {
                            count++;
                        }
                        Assert.Equal(1501, count); // also include the hi lo doc
                    }
                }
            }
        }

        [Fact]
        public void LowLevelExportsByDocPrefixRemote()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 1500; i++)
                    {
                        session.Store(new User());
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var count = 0;
                    using (var streamDocs = session.Advanced.Stream<object>(startsWith: "users/"))
                    {
                        while (streamDocs.MoveNext())
                        {
                            count++;
                        }
                        Assert.Equal(1500, count);
                    }
                }
            }
        }

        [Fact]
        public void HighLevelExportsByDocPrefixRemote()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 1500; i++)
                    {
                        session.Store(new User());
                    }
                    session.SaveChanges();
                }

                int count = 0;
                using (var session = store.OpenSession())
                {
                    using (var reader = session.Advanced.Stream<User>(startsWith: "users/"))
                    {
                        while (reader.MoveNext())
                        {
                            count++;
                            Assert.IsType<User>(reader.Current.Document);
                        }
                    }
                }
                Assert.Equal(1500, count);
            }
        }
    }
}
