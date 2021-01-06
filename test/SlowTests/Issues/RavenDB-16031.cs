using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16031 : RavenTestBase
    {
        [Fact]
        public void ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                new Index_TestGrouping4().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new Account()
                    {
                        Name = "Pier"
                    }, "accounts/100-A");
                    session.SaveChanges();
                }
                WaitForUserToContinueTheTest(store);
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var results = session.Query<Index_TestGrouping4.Result, Index_TestGrouping4>()
                        .OrderBy(x=>x.d)
                        .ProjectInto<Index_TestGrouping4.Result>().ToList();
                    Assert.Equal(3, results.Count);
                    Assert.NotNull(results[0].ac);
                    Assert.NotNull(results[1].ac);
                    Assert.NotNull(results[2].ac);

                    Assert.NotNull(results[0].a.c);
                    Assert.NotNull(results[1].a.c);
                    Assert.NotNull(results[2].a.c);

                    Assert.Equal(5, results[0].d);
                    Assert.Equal(6, results[1].d);
                    Assert.Equal(7, results[2].d);

                    Assert.Equal(3, results[0].a.f);
                    Assert.Equal(4, results[1].a.f);
                    Assert.Equal(3, results[2].a.f);
                }
            }
        }

        private class Account
        {
            public string Name { get; set; }
        }

        private class Index_TestGrouping4 : AbstractJavaScriptIndexCreationTask
        {
            public class Result
            {
                public int e { get; set; }
                public int ab { get; set; }
                public int? ac { get; set; }
                public NestedResult a { get; set; }
                public int d { get; set; }
            }

            public class NestedResult
            {
                public int b { get; set; }
                public int? c { get; set; }
                public int f { get; set; }
            }

            public override string IndexName => "TestGrouping4";
            public Index_TestGrouping4()
            {
                Maps = new HashSet<string>
                {
                    @"map(""Accounts"", (d) => {
    if (id(d) === ""accounts/100-A"") {
        return [
            {
                e: 1,
                ab: 1,
                ac: 2,
                a: {
                    b: 1,
                    c: 2,
                    f: 1
                },
                d: 3
            },
            {
                e: 1,
                ab: 1,
                ac: 2,
                a: {
                    b: 1,
                    c: 2,
                    f: 2
                },
                d: 4
            },
            {
                e: 1,
                ab: 1,
                ac: 3,
                a: {
                    b: 1,
                    c: 3,
                    f: 3
                },
                d: 5
            },
            {
                e: 1,
                ab: 2,
                ac: 2,
                a: {
                    b: 2,
                    c: 2,
                    f: 4
                },
                d: 6
            }
        ]
    }
})"
                };

                Reduce = @"groupBy(x => ({ e: x.e, ab: x.a.b, ac: x.a.c }))
.aggregate(g => { 
    return {
        e: g.key.e,
        ab: g.key.ab,
        ac: g.key.ac,
        a: {
            b: g.key.ab,
            c: g.key.ac
            f: g.values.reduce((res, val) => res + val.a.f, 0)
        },
        d: g.values.reduce((res, val) => res + val.d, 0)
    };
})";
                Fields = new Dictionary<string, IndexFieldOptions>
                {
                    {
                        Constants.Documents.Indexing.Fields.AllFields, new IndexFieldOptions { Storage = FieldStorage.Yes }
                    }
                };
            }
        }

        public RavenDB_16031(ITestOutputHelper output) : base(output)
        {
        }
    }
}
