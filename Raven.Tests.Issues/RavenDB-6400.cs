using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_6400 : RavenTestBase
    {
        public class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        public class User_Index : AbstractIndexCreationTask<User, User_Index.IndexEntry>
        {
            public class IndexEntry
            {
                public string StoredProperty { get; set; }
                public string Name { get; set; }
            }

            public User_Index()
            {
                Map = users => from user in users
                               select new
                               {
                                   Name = user.Name,
                                   StoredProperty = "Bar"
                               };

                Store(e => e.StoredProperty, FieldStorage.Yes);
                Index(e => e.StoredProperty, FieldIndexing.No);
            }
        }

        public class Projected
        {
            public string Name { get; set; }
            public string Foo { get; set; }
        }

        [Fact]
        public void Test()
        {
            using (var store = NewDocumentStore())
            {
                var index = new User_Index();
                index.Execute(store);
                var user = new User { Name = "foo" };

                using (var session = store.OpenSession())
                {
                    session.Store(user);
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                store.DatabaseCommands.Admin.StopIndexing();
                var mre = new ManualResetEventSlim();

                Task.Run(() =>
                {
                    using (var session = store.OpenSession())
                    {
                        session.Advanced.WaitForIndexesAfterSaveChanges();
                        session.Delete(user.Id);
                        session.SaveChanges();
                    }
                    mre.Set();
                });

                //since we stopped indexing, WaitForIndexesAfterSaveChanges and then SaveChanged
                //should block until indexing is started. Therefore, mre shouldn't be set, even after 3 seconds
                Assert.False(mre.Wait(TimeSpan.FromSeconds(3)));

                store.DatabaseCommands.Admin.StartIndexing();
                //now indexing has started, so the index should process the delete, and thus mre should get set
                //(mre should be set in much less than 3 seconds, since there is only one document to index)
                Assert.True(mre.Wait(TimeSpan.FromSeconds(3)));

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User_Index.IndexEntry, User_Index>()
                        .ProjectFromIndexFieldsInto<Projected>()
                        .ToList();

                    // In our application, users would contain the projected user with an empty Name and a non-empty StoredProperty

                    // This works in a unit test setting as indexing is fast enough here
                    Assert.False(users.Any());
                }
            }
        }
    }
}
