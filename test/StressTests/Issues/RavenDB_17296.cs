using System.Linq;
using FastTests;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Issues
{
    public class RavenDB_17296 : RavenTestBase
    {
        public RavenDB_17296(ITestOutputHelper output) : base(output)
        {
        }

        [MultiplatformTheory(RavenPlatform.Windows, RavenArchitecture.AllX64)]
        [InlineData(3_000)]
        [InlineData(10_000)]
        public void Should_Not_Kill_Server_Because_Of_Insufficient_Execution_Stack(int numberOfExpressions)
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var baseQuery = session.Advanced.DocumentQuery<User>();

                    var first = true;
                    for (var i = 0; i < numberOfExpressions; i++)
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

                    var e = Assert.ThrowsAny<RavenException>(() => baseQuery.ToList());
                    Assert.Contains("Query is too complex", e.Message);
                }
            }
        }
    }
}
