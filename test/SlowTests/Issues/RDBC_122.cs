using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RDBC_122 : RavenTestBase
    {
        public RDBC_122(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void IndexShouldCompile()
        {
            using (var store = GetDocumentStore())
            {
                new TestIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc
                    {
                        Month = 10,
                        Year = 1990
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
            }
        }

        private class TestDoc
        {
            public string Id { get; set; }
            public int TestInt { get; set; }
            public int Year { get; set; }
            public int Month { get; set; }
        }

        private class TestIndex : AbstractIndexCreationTask<TestDoc, TestIndex.Queryable>
        {
            public TestIndex()
            {
                Map = accounts => from a in accounts
                                  select new Queryable
                                  {
                                      Id = a.Id,
                                      Test = (int)a.TestInt,
                                      AsAt = new DateTimeOffset((int)a.Year, (int)a.Month, 1, 0, 0, 0, new TimeSpan(0)),
                                  };
            }

            public class Queryable
            {
                public string Id { get; set; }
                public int Test { get; set; }
                public DateTimeOffset AsAt { get; set; }
            }
        }
    }
}
