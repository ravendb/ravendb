using System.Linq;
using FastTests;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class StreamingTests : RavenTestBase
    {
        private class UserFull
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Description { get; set; }
        }

        private class UserLightweight
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [Fact(Skip = "Missing feature: query streaming")]
        public void CanStreamUsingLuceneSelectFields()
        {
            int count = 0;
            using (var store = GetDocumentStore())
            {
                new UserIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        session.Store(new UserFull { Name = "Name " + i, Description = "Description " + i });
                    }
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<UserFull, UserIndex>().SelectFields<UserLightweight>();

                    using (var reader = session.Advanced.Stream(query))
                    {
                        while (reader.MoveNext())
                        {
                            count++;
                            Assert.IsType<UserLightweight>(reader.Current.Document);
                        }
                    }
                }
                Assert.Equal(10, count);
            }
        }

        [Fact(Skip = "Missing feature: query streaming")]
        public void CanGetUsingLuceneSelectFields()
        {
            int count = 0;
            using (var store = GetDocumentStore())
            {
                new UserIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        session.Store(new UserFull { Name = "Name " + i, Description = "Description " + i });
                    }
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<UserFull, UserIndex>().SelectFields<UserLightweight>();

                    using (var reader = query.GetEnumerator())
                    {
                        while (reader.MoveNext())
                        {
                            count++;
                            Assert.IsType<UserLightweight>(reader.Current);
                        }
                    }
                }
                Assert.Equal(10, count);
            }
        }

        private class UserIndex : AbstractIndexCreationTask<UserFull>
        {
            public UserIndex()
            {
                Map = users => from u in users select new { u.Name };
            }
        }
    }

}
