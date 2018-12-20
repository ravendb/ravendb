using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Graph
{
    public class GraphPermissionTests : RavenTestBase
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
with {  from Users where id() = $uid    }   as u

match   (u)<-[Users]-(Issues as i)          or
        (Issues as i)-[Groups]->(Groups as direct)-recursive (0, all) {
            [Parents]->(Groups)
        }->(Groups as g)<-[Groups]-(u)

select  i.Name as Issue, 
        u.Name as User
";

        [Theory]
        [InlineData("users/2944", "Sunny")]
        [InlineData("users/84", "Max")]
        [InlineData("users/12", "Arava")]
        [InlineData("users/23", "Pheobe")]
        public void MultiplePathsWillAllow(string uid, string name)
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);
                using (var s = store.OpenSession())
                {
                    var r = s.Advanced.RawQuery<Result>(Query)
                        .AddParameter("uid", uid)
                        .Single();

                    Assert.Equal("Design a logo of the project (red)", r.Issue);
                    Assert.Equal(name, r.User);
                }
            }
        }

        [Theory]
        [InlineData("users/4931")]
        public void NoPathWillDeny(string uid)
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);
                using (var s = store.OpenSession())
                {
                    var r = s.Advanced.RawQuery<Result>(Query)
                        .AddParameter("uid", uid)
                        .SingleOrDefault();

                    Assert.Null(r);
                }
            }
        }

        [Fact]
        public void Edge_instead_of_node_should_make_simple_left_to_right_query_fail()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);
                using (var s = store.OpenSession())
                {
                    Assert.Throws<InvalidQueryException>(() => s.Advanced.RawQuery<Result>("match (Issues as i)-[Groups]->[Groups as direct]").ToList());
                }
            }

        }

        [Fact]
        public void Node_instead_of_edge_should_make_simple_left_to_right_query_fail()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);
                using (var s = store.OpenSession())
                {
                    //different type of mistake - writing node instead of edge
                    Assert.Throws<InvalidQueryException>(() => s.Advanced.RawQuery<Result>("match (Issues as i)-(Groups)->(Groups as direct)").ToList());
                }
            }
        }

        [Fact]
        public void Edge_instead_of_node_should_make_simple_right_to_left_query_fail()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);
                using (var s = store.OpenSession())
                {
                    Assert.Throws<InvalidQueryException>(() => s.Advanced.RawQuery<Result>("match (Groups as A)<-[Groups as B]<-[Issues as C]").ToList());
                }
            }
        }

        [Fact]
        public void Node_instead_of_edge_should_make_simple_right_to_left_query_fail()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);
                using (var s = store.OpenSession())
                {
                    Assert.Throws<InvalidQueryException>(() => s.Advanced.RawQuery<Result>("match (Groups as A)<-(Groups as B)<-(Issues as C)").ToList());
                }
            }
        }

        [Fact]
        public void Edge_instead_of_node_should_make_complex_query_fail()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);
                using (var s = store.OpenSession())
                {
                    Assert.Throws<InvalidQueryException>(() => s.Advanced.RawQuery<Result>(
                        @"with {  from Users where id() = ""users/84"" } as u
                          match   (Issues as i)-[Groups]->(Groups as g)<-[Parents]-[Groups as midpoint]<-[Groups]-(u)
                          select  i.Name as Issue, u.Name as User").ToList());
                }
            }
        }
    }
}
