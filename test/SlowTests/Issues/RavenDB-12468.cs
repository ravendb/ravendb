using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using SlowTests.Graph;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12468 : RavenTestBase
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
                session.Store(new GraphPermissionTests.User { Name = "Sunny", Title  = "Designer" }, "users/2944");
                session.Store(new GraphPermissionTests.User { Name = "Max", Title = "Project Manager", Groups = new[] { "groups/project-x" } }, "users/84");
                session.Store(new GraphPermissionTests.User { Name = "Arava", Title = "CEO", Groups = new[] { "groups/execs" } }, "users/12");
                session.Store(new GraphPermissionTests.User { Name = "Oscar", Title = "CTO", Groups = new[] { "groups/execs" } }, "users/62");
                session.Store(new GraphPermissionTests.User { Name = "Pheobe", Title = "Director", Groups = new[] { "groups/board" } }, "users/23");
                session.Store(new GraphPermissionTests.User { Name = "Nati", Title = "Team Lead", Groups = new[] { "groups/team-nati" } }, "users/341");
                session.Store(new GraphPermissionTests.User { Name = "Bert", Title = "Developer", Groups = new[] { "groups/team-nati" } }, "users/4193");
                session.Store(new GraphPermissionTests.User { Name = "Yamit", Title = "Developer", Groups = new[] { "groups/team-nati" } }, "users/3432");
                session.Store(new GraphPermissionTests.User { Name = "Snoopy", Title = "Fellow", Groups = new[] { "groups/r-n-d" } }, "users/4931");

                session.Store(new GraphPermissionTests.Group { Name = "Project X (secret)", Parents = new[] { "groups/execs", "groups/team-nati" } }, "groups/project-x");
                session.Store(new GraphPermissionTests.Group { Name = "Executives", Parents = new[] { "groups/board" } }, "groups/execs");
                session.Store(new GraphPermissionTests.Group { Name = "Nati's Team", }, "groups/team-nati");
                session.Store(new GraphPermissionTests.Group { Name = "Board of Directors" }, "groups/board");
                session.Store(new GraphPermissionTests.Group { Name = "R&D Department", Parents = new[] { "groups/execs" } }, "groups/r-n-d");


                session.Store(new GraphPermissionTests.Issue { Name = "Design a logo of the project (red)", Groups = new[] { "groups/project-x" }, Users = new[] { "users/2944" } });

                session.SaveChanges();
            }
        }

        [Fact]
        public void Should_support_id_operator_in_simple_select()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);
                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<JObject>(@"
                                        with {  from Users where id() = 'users/341'    }   as u
                                        match (Issues as i)-[Groups]->(Groups as direct)-recursive {
                                                    [Parents]->(Groups)
                                        }->(Groups as g)<-[Groups]-(u)
                                        select  i.Name as Issue,  u.Name as User, id(u) as UserId
                                    ").First();

                    Assert.Equal("users/341",result["UserId"].Value<string>());
                }
            }
        }
    }
}
