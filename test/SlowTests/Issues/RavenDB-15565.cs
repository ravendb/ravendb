using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15565 : RavenTestBase
    {
        public RavenDB_15565(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Name;
        }

        public class Group
        {
            public string[] Groups;
            public string[] Users;
        }

        public class Index : AbstractIndexCreationTask<Group>
        {
            public Index()
            {
                Map = groups => from grp in groups
                    let users = grp.Groups.Select(LoadDocument<Group>).Concat(new[] {grp}).SelectMany(x => x.Users).Select(LoadDocument<User>)
                    select new {Users = users.Select(x => x.Name)};
            }

        }

        [Fact]
        public void WillNotReindexSameDocumentMultipleTimes()
        {
            using var store = GetDocumentStore();
            using (var session = store.OpenSession())
            {
                session.Store(new Group
                {
                    Groups = new[] { "groups/2" },
                    Users = new[] { "users/1" }
                }, "groups/1");

                session.Store(new Group
                {
                    Groups = new[] { "groups/3" },
                    Users = new[] { "users/2" }
                }, "groups/2");

                session.Store(new Group
                {
                    Users = new[] { "users/1" }
                }, "groups/3");
                session.Store(new User { Name = "a" }, "users/1");
                session.Store(new User { Name = "b" }, "users/2");


                session.SaveChanges();
            }

            new Index().Execute(store);
            WaitForIndexing(store);

            IndexStats stats = store.Maintenance.Send(new GetIndexStatisticsOperation("index"));
            Assert.Equal(3, stats.EntriesCount);
            Assert.Equal(3, stats.MapAttempts);
            Assert.Equal(0, stats.MapReferenceAttempts);
        }
    }
}
