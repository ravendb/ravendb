// -----------------------------------------------------------------------
//  <copyright file="RavenDB820.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB820 : RavenTestBase
    {
        public RavenDB820(ITestOutputHelper output) : base(output)
        {
        }

        private class Foo
        {
            public string First { get; set; }
        }

        private class TestIndex : AbstractIndexCreationTask<Foo, TestIndex.QueryResult>
        {
            public class QueryResult
            {
                public string Query { get; set; }
            }

            public class ActualResult
            {
                public string[] Query { get; set; }
            }

            public TestIndex()
            {
                Map = docs => docs.Select(doc => new
                {
                    Query = new object[]
                    {
                        doc.First
                    },
                });
                Index(org => org.Query, FieldIndexing.Search);
                Store(org => org.Query, FieldStorage.Yes);
            }
        }

        [Fact]
        public void CanGetProjectionOfMixedContent()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new TestIndex());
                using (var session = store.OpenSession())
                {
                    session.Store(new Foo { First = "foo" });
                    session.Store(new Foo { First = "foo2" });
                    session.SaveChanges();

                    var a = session.Query<TestIndex.QueryResult, TestIndex>()
                                   .Customize(c => c.WaitForNonStaleResults())
                                   .Where(r => r.Query.StartsWith("foo"))
                                   .ProjectInto<TestIndex.ActualResult>()
                                   .ToList();
                    Assert.NotEmpty(a);
                }
            }
        }
    }
}
