using System.Linq;
using FastTests;
using FastTests.Server.Basic.Entities;
using Xunit;

namespace SlowTests.Tests.Bugs.Vlko
{
    public class QueryWithMultipleWhere : RavenNewTestBase
    {
        [Fact]
        public void ShouldGenerateProperPrecedence()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    var query = s.Query<User>()
                        .Where(x => x.Id == "1" || x.Id == "2" || x.Id == "3")
                        .Where(x => x.Age == 19)
                        .ToString();

                    Assert.Equal("((__document_id:1 OR __document_id:2) OR __document_id:3) AND (Age:19)", query);
                }
            }
        }
    }
}
