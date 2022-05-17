using Tests.Infrastructure;
using System.Linq;
using FastTests;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10490 : RavenTestBase
    {
        public RavenDB_10490(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void Can_Load_Inside_Let_With_Argument_Predefined_in_Let(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Jerry",
                        LastName = "Garcia",
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
                                let detailId = "details/1-A"
                                let name = u.Name
                                let detail = session.Load<Detail>(detailId)
                                select new
                                {
                                    Name = name,
                                    DetailId = detailId,
                                    Detail = detail
                                };

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(u) {
    var detailId = ""details/1-A"";
    var name = u?.Name;
    var detail = load(detailId);
    return { Name : name, DetailId : detailId, Detail : detail };
}
from 'Users' as u select output(u)", query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(1, queryResult.Count);
                    Assert.Equal("Jerry", queryResult[0].Name);
                    Assert.Equal("details/1-A", queryResult[0].DetailId);
                    Assert.Equal(15, queryResult[0].Detail.Number);
                }
            }
        }

        private class Detail
        {
            public int Number { get; set; }
        }
    }
}
