using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using SlowTests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8213 : RavenTestBase
    {

        [Fact]
        public void SyntaxErrorInDeclareFunctionShouldReportLineAndColumnNumber()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var projection = session.Advanced.RawQuery<object>(
                        @"declare function f(x){
    if (x.Lines.length > 0))
        x.Company = 'new';
    return x;
}
from Orders as o select f(o)");

                    var ex = Assert.Throws<InvalidQueryException>(() => projection.ToList());
                    Assert.Contains("At Line : 2, Column : 28", ex.Message);
                }
            }
        }

        [Fact]
        public void SyntaxErrorInSelectFunctionBodyShouldReportLineAndColumnNumber()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var projection = session.Advanced.RawQuery<object>(
                        @"from Orders as o select {
    Company : o.Company,
    Lines : o..Lines
}");

                    var ex = Assert.Throws<InvalidQueryException>(() => projection.ToList());
                    Assert.Contains("At Line : 3, Column : 15", ex.Message);
                }
            }
        }

        [Fact]
        public async Task SyntaxErrorInPatchScriptShouldReportLineAndColumnNumber()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var op = store.Operations.SendAsync(new PatchByQueryOperation(@"
from Users as u
update
{
    del(id(u));
    this[""@metadata""][""@collection""] = ""People"";
    put(id(u), this));
}
"));

                    var ex = await Assert.ThrowsAsync<InvalidQueryException>(async () => await op);
                    Assert.Contains("At Line : 5, Column : 21", ex.Message);
                }
            }
        }

        [Fact]
        public void ErrorInInvocationShouldReportLineAndColumnNumber()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Count = 2
                    });
                    session.SaveChanges();

                }
                using (var session = store.OpenSession())
                {
                    var projection = session.Advanced.RawQuery<object>(
@"from Users as u select {
    CountStr : u.Count.toString(40)
}");

                    var ex = Assert.Throws<Raven.Client.Exceptions.Documents.Patching.JavaScriptException>(() => projection.ToList());
                    Assert.Contains("at toString(40) @  15:3", ex.Message);
                }
            }
        }
    }
}
