using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15693 : RavenTestBase
    {
        private class Doc
        {
#pragma warning disable 649
            public string StrVal1, StrVal2, StrVal3;
#pragma warning restore 649
        }

        [Fact]
        public void CanQueryOnComplexBoost()
        {
            using var store = GetDocumentStore();
            using var s = store.OpenSession();

            var q = s.Advanced.DocumentQuery<Doc>()
                .Search(x => x.StrVal1, "a")
                .AndAlso()
                .OpenSubclause()
                .Search(x => x.StrVal2, "b")
                .OrElse()
                .Search(x => x.StrVal3, "search")
                .CloseSubclause()
                .Boost(0.2m);
            var queryBoost = q.ToString();

            Assert.Equal("from 'Docs' where search(StrVal1, $p0) and boost(search(StrVal2, $p1) or search(StrVal3, $p2), $p3)", queryBoost);

            q.ToList();
        }

        public RavenDB_15693(ITestOutputHelper output) : base(output)
        {
        }
    }
}
