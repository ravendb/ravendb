using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13656 : RavenTestBase
    {
        public RavenDB_13656(ITestOutputHelper output) : base(output)
        {
        }

        private class TestDoc
        {
            public long Limit { get; set; }

            public long Offset { get; set; }
        }

        [Fact]
        public void CanProjectPropertyCalledLimit()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc
                    {
                        Limit = 1
                    });

                    session.Store(new TestDoc
                    {
                        Limit = 2
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var q = session.Query<TestDoc>()
                        .Select(x => new
                        {
                            x.Limit
                        });

                    Assert.Equal("from 'TestDocs' as __alias0 select __alias0.'Limit'", q.ToString());

                    var results = q.ToList();

                    Assert.Equal(2, results.Count);

                    Assert.Equal(1, results[0].Limit);
                    Assert.Equal(2, results[1].Limit);
                }

            }
        }

        [Fact]
        public void CanProjectPropertyCalledOffset()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc
                    {
                        Offset = 1
                    });

                    session.Store(new TestDoc
                    {
                        Offset = 2
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var q = session.Query<TestDoc>()
                        .Select(x => new
                        {
                            x.Offset
                        });

                    Assert.Equal("from 'TestDocs' as __alias0 select __alias0.'Offset'", q.ToString());

                    var results = q.ToList();

                    Assert.Equal(2, results.Count);

                    Assert.Equal(1, results[0].Offset);
                    Assert.Equal(2, results[1].Offset);
                }

            }
        }
    }
}
