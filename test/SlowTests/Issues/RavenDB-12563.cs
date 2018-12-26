using FastTests;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
using SlowTests.Graph;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12563 : RavenTestBase
    {
          public class User
        {
            public string Name;
            public string Title;
            public string[] Groups;
        }

        public class Group
        {
            public string Name;
            public string[] Parents;
        }

        public class Issue
        {
            public string Name;
            public string[] Users;
            public string[] Groups;
        }

        private static void CreateData(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "Sunny", Title  = "Designer" }, "users/2944");
                session.Store(new User { Name = "Max", Title = "Project Manager", Groups = new[] { "groups/project-x" } }, "users/84");
                session.Store(new User { Name = "Arava", Title = "CEO", Groups = new[] { "groups/execs" } }, "users/12");
                session.Store(new User { Name = "Oscar", Title = "CTO", Groups = new[] { "groups/execs" } }, "users/62");
                session.Store(new User { Name = "Pheobe", Title = "Director", Groups = new[] { "groups/board" } }, "users/23");
                session.Store(new User { Name = "Nati", Title = "Team Lead", Groups = new[] { "groups/team-nati" } }, "users/341");
                session.Store(new User { Name = "Bert", Title = "Developer", Groups = new[] { "groups/team-nati" } }, "users/4193");
                session.Store(new User { Name = "Yamit", Title = "Developer", Groups = new[] { "groups/team-nati" } }, "users/3432");
                session.Store(new User { Name = "Snoopy", Title = "Fellow", Groups = new[] { "groups/r-n-d" } }, "users/4931");

                session.Store(new Group { Name = "Project X (secret)", Parents = new[] { "groups/execs", "groups/team-nati" } }, "groups/project-x");
                session.Store(new Group { Name = "Executives", Parents = new[] { "groups/board" } }, "groups/execs");
                session.Store(new Group { Name = "Nati's Team", }, "groups/team-nati");
                session.Store(new Group { Name = "Board of Directors" }, "groups/board");
                session.Store(new Group { Name = "R&D Department", Parents = new[] { "groups/execs" } }, "groups/r-n-d");


                session.Store(new Issue { Name = "Design a logo of the project (red)", Groups = new[] { "groups/project-x" }, Users = new[] { "users/2944" } });

                session.SaveChanges();
            }
        }

        public class Result
        {
            public string Issue;
            public string User;
        }

        const string Query = @"
with {  from Users where id() = 'users/84' } as u
match   (u)<-[Users]-(Issues as i) or
        (Issues as i)-[Groups]->(Groups as direct)-recursive (0, all) { [Parents]->(Groups) }<-[Groups]-(u)

select  i.Name as Issue, 
        u.Name as User
";

        [Fact]
        public void Should_throw_error_because_invalid_query()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);
                using (var session = store.OpenSession())
                {
                    Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<GraphPermissionTests.Result>(Query).Single());
                }
            }
        }
    }
}
