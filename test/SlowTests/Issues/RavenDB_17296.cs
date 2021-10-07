using System.Linq;
using FastTests;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17296 : RavenTestBase
    {
        public RavenDB_17296(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Should_Not_Kill_Server_Because_Of_Not_Sufficient_Execution_Stack()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var baseQuery = session.Advanced.DocumentQuery<User>();

                    var first = true;
                    for (var i = 0; i < 1_000; i++)
                    {
                        if (first == false)
                            baseQuery.OrElse();

                        baseQuery.OpenSubclause()
                            .WhereEquals(x => x.Name, "grisha")
                            .AndAlso()
                            .WhereEquals(x => x.Name, "grisha")
                            .AndAlso()
                            .WhereNotEquals(x => x.Name, "1")
                            .CloseSubclause();

                        first = false;
                    }

                    var e = Assert.Throws<RavenException>(() => baseQuery.ToList());
                    Assert.Contains("InsufficientExecutionStackException", e.Message);
                }
            }
        }
    }
}
