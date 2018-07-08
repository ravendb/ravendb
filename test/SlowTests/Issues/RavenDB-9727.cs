using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_9727 : RavenTestBase
    {
        [Fact]
        public void Can_Load_with_Argument_that_has_String_Interpolation()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Jerry",
                        LastName = "Garcia",
                        DetailShortId = "1-A"
                    }, "users/1");
                    session.Store(new Detail
                    {
                        Number = 15
                    }, "details/1-A");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                                where u.LastName == "Garcia"
                                let detail = session.Load<Detail>("details/"+u.DetailShortId)
                                select new
                                {
                                    Name = u.Name,
                                    Detail = detail
                                };

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(u) {
	var detail = load((""details/""+u.DetailShortId));
	return { Name : u.Name, Detail : detail };
}
from Users as u where u.LastName = $p0 select output(u)", query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jerry", queryResult[0].Name);
                    Assert.Equal(15, queryResult[0].Detail.Number);

                }
            }
        }

        [Fact]
        public void Can_Load_inside_Select_with_Argument_Predefined_in_Let_that_has_Complex_String_Interpolation()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Jerry",
                        LastName = "Garcia",
                        DetailShortId = "1"
                    }, "users/1");
                    session.Store(new Detail
                    {
                        Number = 15
                    }, "details/1-A");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var docInfo = new
                    {
                        Node = "A",
                        Seperator = "/"
                    };
                  
                    var query = from u in session.Query<User>()
                                let detailId = "d"+u.Name.ElementAt(1)+"ta"+u.LastName.ElementAt(4)+"ls"+docInfo.Seperator+u.DetailShortId+"-"+docInfo.Node
                                select new
                                {
                                    Name = u.Name,
                                    DetailId = detailId,
                                    Detail = session.Load<Detail>(detailId)
                                };

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(u, $p0, $p1) {
	var detailId = ""d""+u.Name[1]+""ta""+u.LastName[4]+""ls""+$p0+u.DetailShortId+""-""+$p1;
	return { Name : u.Name, DetailId : detailId, Detail : load(detailId) };
}
from Users as u select output(u, $p0, $p1)", query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jerry", queryResult[0].Name);
                    Assert.Equal("details/1-A", queryResult[0].DetailId);
                    Assert.Equal(15, queryResult[0].Detail.Number);

                }
            }
        }

        [Fact]
        public void Can_Load_inside_Let_with_Argument_Predefined_in_Let_that_has_String_Interpolation()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Jerry",
                        DetailShortId = "1-A"
                    }, "users/1");
                    session.Store(new Detail
                    {
                        Number = 15
                    }, "details/1-A");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from u in session.Query<User>()
                                let detailId = "details/"+u.DetailShortId
                                let detail = session.Load<Detail>(detailId)
                                select new
                                {
                                    Name = u.Name,
                                    Detail = detail
                                };

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(u) {
	var detailId = ""details/""+u.DetailShortId;
	var detail = load(detailId);
	return { Name : u.Name, Detail : detail };
}
from Users as u select output(u)", query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jerry", queryResult[0].Name);
                    Assert.Equal(15, queryResult[0].Detail.Number);

                }
            }
        }

        private class User
        {
            public string Name { get; set; }
            public string LastName { get; set; }
            public string DetailShortId { get; set; }
        }
        private class Detail
        {
            public int Number { get; set; }
        }
    }
}
