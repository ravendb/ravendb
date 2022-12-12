using System.Linq;
using FastTests;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10625 : RavenTestBase
    {
        public RavenDB_10625(ITestOutputHelper output) : base(output)
        {
        }

        private class Article
        {
            public int? Quantity { get; set; }
        }

        private class Referenced
        {
            public string Id { get; set; }
        }

        [Fact]
        public void CanTranslateGroupsCorrectly()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Article());
                    session.Store(new Referenced { Id = "referenced-id"});
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from x in session.Query<Article>()
                        let test = 1
                        let valid = RavenQuery.Load<Referenced>("referenced-id")
                        let missing = RavenQuery.Load<Referenced>("should-not-exist")
                        select new
                        {
                            CheckGroup = ((x.Quantity ?? 0) != 0 ? 2 : 3) == 2 ? 1 : 0,
                            CheckGroup1 = (x.Quantity == null ? 1 : 2) == 1 ? 1 : 2,
                            CheckGroup2 = (x.Quantity ?? 0),
                            CheckGroup3 = x.Quantity ?? 0,
                            CheckGroup4 = ((x.Quantity ?? 0)) != 0 ? 2 : 3,
                            CheckGroup5 = x.Quantity != null ? x.Quantity : 0,
                            CheckGroup6 = valid ?? (missing != null ? missing : valid)
                        };

                    const string expected = @"declare function output(x) {
	var test = 1;
	var valid = load(""referenced-id"");
	var missing = load(""should-not-exist"");
	return { CheckGroup : ((x.Quantity??0)!==0?2:3)===2?1:0, CheckGroup1 : (x.Quantity==null?1:2)===1?1:2, CheckGroup2 : (x.Quantity??0), CheckGroup3 : (x.Quantity??0), CheckGroup4 : (x.Quantity??0)!==0?2:3, CheckGroup5 : x.Quantity!=null?x.Quantity:0, CheckGroup6 : (valid??(missing!=null?missing:valid)) };
}
from 'Articles' as x select output(x)";

                    RavenTestHelper.AssertEqualRespectingNewLines(expected, query.ToString());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(0, result[0].CheckGroup);
                    Assert.Equal(1, result[0].CheckGroup1);
                    Assert.Equal(0, result[0].CheckGroup2);
                    Assert.Equal(0, result[0].CheckGroup3);
                    Assert.Equal(3, result[0].CheckGroup4);
                    Assert.Equal(0, result[0].CheckGroup5);
                    Assert.Equal("referenced-id", result[0].CheckGroup6?.Id);
                }
            }
        }
    }
}
