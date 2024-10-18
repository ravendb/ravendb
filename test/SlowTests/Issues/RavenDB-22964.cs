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
    
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public void TestOrderByDescendingForStrings(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var t1 = new TestObject() { StringValue = "3" };
                var t2 = new TestObject() { StringValue = "5" };
                var t3 = new TestObject() { StringValue = "6" };
                var t4 = new TestObject() { StringValue = "7" };
                
                session.Store(t1);
                session.Store(t2);
                session.Store(t3);
                session.Store(t4);
                session.SaveChanges();
                
                string min = "4";
                string max = "6";
                
                var r0 = session.Advanced.RawQuery<TestObject>("from 'TestObjects' where StringValue >= $p0 and StringValue < $p1 order by StringValue desc")
                    .AddParameter("$p0", min)
                    .AddParameter("$p1", max)
                    .ToList();

                Assert.Equal(1, r0.Count);
                Assert.Equal("5", r0[0].StringValue);
                
                var r1 = session.Advanced.RawQuery<TestObject>("from 'TestObjects' where StringValue > $p0 and StringValue < $p1 order by StringValue desc")
                    .AddParameter("$p0", min)
                    .AddParameter("$p1", max)
                    .ToList();

                Assert.Equal(1, r1.Count);
                Assert.Equal("5", r1[0].StringValue);
                
                var r2 = session.Advanced.RawQuery<TestObject>("from 'TestObjects' where StringValue > $p0 and StringValue <= $p1 order by StringValue desc")
                    .AddParameter("$p0", min)
                    .AddParameter("$p1", max)
                    .ToList();
                
                Assert.Equal(2, r2.Count);
                Assert.Equal("6", r2[0].StringValue);
                Assert.Equal("5", r2[1].StringValue);
                
                var r3 = session.Advanced.RawQuery<TestObject>("from 'TestObjects' where StringValue >= $p0 and StringValue <= $p1 order by StringValue desc")
                    .AddParameter("$p0", min)
                    .AddParameter("$p1", max)
                    .ToList();
                
                Assert.Equal(2, r3.Count);
                Assert.Equal("6", r3[0].StringValue);
                Assert.Equal("5", r3[1].StringValue);

                min = "2";
                
                var r4 = session.Advanced.RawQuery<TestObject>("from 'TestObjects' where StringValue > $p0 and StringValue < $p1 order by StringValue desc")
                    .AddParameter("$p0", min)
                    .AddParameter("$p1", max)
                    .ToList();
                
                Assert.Equal(2, r4.Count);
                Assert.Equal("5", r4[0].StringValue);
                Assert.Equal("3", r4[1].StringValue);

                max = "9";
                
                var r5 = session.Advanced.RawQuery<TestObject>("from 'TestObjects' where StringValue > $p0 and StringValue < $p1 order by StringValue desc")
                    .AddParameter("$p0", min)
                    .AddParameter("$p1", max)
                    .ToList();
                
                Assert.Equal(4, r5.Count);
                Assert.Equal("7", r5[0].StringValue);
                Assert.Equal("6", r5[1].StringValue);
                Assert.Equal("5", r5[2].StringValue);
                Assert.Equal("3", r5[3].StringValue);
                
                min = "9";
                max = "90";
                
                var r6 = session.Advanced.RawQuery<TestObject>("from 'TestObjects' where StringValue > $p0 and StringValue < $p1 order by StringValue desc")
                    .AddParameter("$p0", min)
                    .AddParameter("$p1", max)
                    .ToList();
                
                Assert.Equal(0, r6.Count);
                
                min = "0";
                max = "2";
                
                var r7 = session.Advanced.RawQuery<TestObject>("from 'TestObjects' where StringValue > $p0 and StringValue < $p1 order by StringValue desc")
                    .AddParameter("$p0", min)
                    .AddParameter("$p1", max)
                    .ToList();
                
                Assert.Equal(0, r7.Count);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public void TestRangeBetweenValuesForStrings(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var t1 = new TestObject() { StringValue = "2" };
                var t2 = new TestObject() { StringValue = "8" };

                session.Store(t1);
                session.Store(t2);
                session.SaveChanges();

                const string min = "4";
                const string max = "6";
                
                var r1 = session.Advanced.RawQuery<TestObject>("from 'TestObjects' where StringValue > $p0 and StringValue < $p1 order by StringValue desc")
                    .AddParameter("$p0", min)
                    .AddParameter("$p1", max)
                    .ToList();

                Assert.Equal(0, r1.Count);
                
                var r2 = session.Advanced.RawQuery<TestObject>("from 'TestObjects' where StringValue >= $p0 and StringValue < $p1 order by StringValue desc")
                    .AddParameter("$p0", min)
                    .AddParameter("$p1", max)
                    .ToList();
                
                Assert.Equal(0, r2.Count);
                
                var r3 = session.Advanced.RawQuery<TestObject>("from 'TestObjects' where StringValue > $p0 and StringValue <= $p1 order by StringValue desc")
                    .AddParameter("$p0", min)
                    .AddParameter("$p1", max)
                    .ToList();
                
                Assert.Equal(0, r3.Count);
                
                var r4 = session.Advanced.RawQuery<TestObject>("from 'TestObjects' where StringValue >= $p0 and StringValue <= $p1 order by StringValue desc")
                    .AddParameter("$p0", min)
                    .AddParameter("$p1", max)
                    .ToList();
                
                Assert.Equal(0, r4.Count);
            }
        }
    }
    
    private class TestObject
    {
        public long Value { get; set; }
        public string StringValue { get; set; }
    }
}
