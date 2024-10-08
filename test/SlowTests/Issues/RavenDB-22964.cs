using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22964 : RavenTestBase
{
    public RavenDB_22964(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public void TestOrderByDescending(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var t1 = new TestObject() { Value = 1 };
                var t2 = new TestObject() { Value = 3 };
                var t3 = new TestObject() { Value = 4 };
                var t4 = new TestObject() { Value = 5 };
                
                session.Store(t1);
                session.Store(t2);
                session.Store(t3);
                session.Store(t4);
                session.SaveChanges();
                
                long min = 2;
                long max = 4;
                
                var r0 = session.Query<TestObject>()
                    .Where(x => x.Value >= min && x.Value < max)
                    .OrderByDescending(x => x.Value)
                    .ToList();

                Assert.Equal(1, r0.Count);
                Assert.Equal(3, r0[0].Value);
                
                var r1 = session.Query<TestObject>()
                    .Where(x => x.Value > min && x.Value < max)
                    .OrderByDescending(x => x.Value)
                    .ToList();

                Assert.Equal(1, r1.Count);
                Assert.Equal(3, r1[0].Value);
                
                var r2 = session.Query<TestObject>()
                    .Where(x => x.Value > min && x.Value <= max)
                    .OrderByDescending(x => x.Value)
                    .ToList();
                
                Assert.Equal(2, r2.Count);
                Assert.Equal(4, r2[0].Value);
                Assert.Equal(3, r2[1].Value);
                
                var r3 = session.Query<TestObject>()
                    .Where(x => x.Value >= min && x.Value <= max)
                    .OrderByDescending(x => x.Value)
                    .ToList();
                
                Assert.Equal(2, r3.Count);
                Assert.Equal(4, r3[0].Value);
                Assert.Equal(3, r3[1].Value);

                min = 0;
                
                var r4 = session.Query<TestObject>()
                    .Where(x => x.Value > min && x.Value < max)
                    .OrderByDescending(x => x.Value)
                    .ToList();
                
                Assert.Equal(2, r4.Count);
                Assert.Equal(3, r4[0].Value);
                Assert.Equal(1, r4[1].Value);

                max = 7;
                
                var r5 = session.Query<TestObject>()
                    .Where(x => x.Value > min && x.Value < max)
                    .OrderByDescending(x => x.Value)
                    .ToList();
                
                Assert.Equal(4, r5.Count);
                Assert.Equal(5, r5[0].Value);
                Assert.Equal(4, r5[1].Value);
                Assert.Equal(3, r5[2].Value);
                Assert.Equal(1, r5[3].Value);
                
                min = 10;
                max = 15;
                
                var r6 = session.Query<TestObject>()
                    .Where(x => x.Value > min && x.Value < max)
                    .OrderByDescending(x => x.Value)
                    .ToList();
                
                Assert.Equal(0, r6.Count);
                
                min = -5;
                max = -2;
                
                var r7 = session.Query<TestObject>()
                    .Where(x => x.Value > min && x.Value < max)
                    .OrderByDescending(x => x.Value)
                    .ToList();
                
                Assert.Equal(0, r7.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public void TestRangeBetweenValues(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var t1 = new TestObject() { Value = 2 };
                var t2 = new TestObject() { Value = 24 };

                session.Store(t1);
                session.Store(t2);
                session.SaveChanges();

                const long min = 6;
                const long max = 12;

                var r1 = session.Query<TestObject>()
                    .Where(x => x.Value > min && x.Value < max)
                    .OrderByDescending(x => x.Value)
                    .ToList();

                Assert.Equal(0, r1.Count);
                
                var r2 = session.Query<TestObject>()
                    .Where(x => x.Value >= min && x.Value < max)
                    .OrderByDescending(x => x.Value)
                    .ToList();
                
                Assert.Equal(0, r2.Count);
                
                var r3 = session.Query<TestObject>()
                    .Where(x => x.Value > min && x.Value <= max)
                    .OrderByDescending(x => x.Value)
                    .ToList();
                
                Assert.Equal(0, r3.Count);
                
                var r4 = session.Query<TestObject>()
                    .Where(x => x.Value >= min && x.Value <= max)
                    .OrderByDescending(x => x.Value)
                    .ToList();
                
                Assert.Equal(0, r4.Count);
            }
        }
    }
    
    private class TestObject
    {
        public required long Value { get; set; }
    }
}
