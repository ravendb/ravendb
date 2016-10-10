using System;
using System.Linq;
using FastTests;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using SlowTests.Utils;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_5434 : RavenTestBase
    {
        private class User
        {
            public string Name { get; set; }

            public DateTime CreatedDate { get; set; }

            public DateTime OtherDate { get; set; }
        }

        private class Users_ByName : AbstractIndexCreationTask<User>
        {
            public class Result
            {
                public string Name { get; set; }

                public TimeSpan Time { get; set; }
            }

            public Users_ByName()
            {
                Map = users => from u in users
                               select new
                               {
                                   Name = u.Name,
                                   Time = u.OtherDate - u.CreatedDate
                               };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        [Fact]
        public void CanIndexTimeSpan()
        {
            using (var store = GetDocumentStore())
            {
                new Users_ByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    var now = DateTime.Now;

                    session.Store(new User
                    {
                        Name = "John",
                        CreatedDate = now,
                        OtherDate = now.AddDays(1)
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);
                TestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<User, Users_ByName>()
                        .ProjectFromIndexFieldsInto<Users_ByName.Result>()
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal("John", results[0].Name);
                    Assert.Equal(new TimeSpan(1, 0, 0, 0), results[0].Time);
                }
            }
        }
    }
}