using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using FastTests.Graph;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_18580 : RavenTestBase
    {
        public RavenDB_18580(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task MyTest()
        {
            int docsCount = 5;
            var arr = new TestObj[docsCount];
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.MaxNumberOfRequestsPerSession = 60_000;
                    for (int i = 0; i < docsCount; i++)
                    {
                        arr[i] = new TestObj {Field1 = $"{i}1", Field2 = $"{i}2"};
                        await session.StoreAsync(arr[i]);
                        await session.SaveChangesAsync();
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    var q0 = session.Query<TestObj>()
                        .Search(o => o.Field1, new List<string>() {"*"});
                    var l0 = await q0.ToListAsync();
                    AssertQueryResults(arr, l0);
                    var indexes0 = store.Maintenance.Send(new GetStatisticsOperation()).Indexes;
                    Assert.Equal(0, indexes0.Length);


                    var q1 = session.Query<TestObj>()
                        .Search(o => o.Field1, new List<string>() {"*"})
                        .Search(o => o.Field2, new List<string>() { "22" });
                      var l1 = await q1.ToListAsync();
                    AssertQueryResults(arr, l1);
                    var indexes1 = store.Maintenance.Send(new GetStatisticsOperation()).Indexes;
                    Assert.Equal(1, indexes1.Length);
                    Assert.Equal("Auto/TestObjs/BySearch(Field1)AndSearch(Field2)", indexes1[0].Name);

                    var q2 = session.Query<TestObj>()
                        .Search(o => o.Field1, new List<string>() { "11" })
                        .Search(o => o.Field2, new List<string>() { "22" });
                    var l2 = await q2.ToListAsync();
                    AssertQueryResults(new TestObj[] { arr[1], arr[2] }, l2);
                    var indexes2 = store.Maintenance.Send(new GetStatisticsOperation()).Indexes;
                    Assert.Equal(1, indexes2.Length);
                    Assert.Equal("Auto/TestObjs/BySearch(Field1)AndSearch(Field2)", indexes2[0].Name);

                    var q3 = session.Query<TestObj>()
                        .Search(o => o.Field1, new List<string>() { "11" })
                        .Search(o => o.Field2, new List<string>() { "*" });
                    var l3 = await q3.ToListAsync();
                    AssertQueryResults(arr, l3);
                    var indexes3 = store.Maintenance.Send(new GetStatisticsOperation()).Indexes;
                    Assert.Equal(1, indexes3.Length);
                    Assert.Equal("Auto/TestObjs/BySearch(Field1)AndSearch(Field2)", indexes3[0].Name);

                    var q4 = session.Query<TestObj>()
                        .Search(o => o.Field2, new List<string>() {"22"})
                        .Search(o => o.Field1, new List<string>() {"41"});
                    var l4 = await q4.ToListAsync();
                    AssertQueryResults(new TestObj[] { arr[2], arr[4] }, l4);
                    var indexes4 = store.Maintenance.Send(new GetStatisticsOperation()).Indexes;
                    Assert.Equal(1, indexes4.Length);
                    Assert.Equal("Auto/TestObjs/BySearch(Field1)AndSearch(Field2)", indexes4[0].Name);

                    var q5 = session.Query<TestObj>()
                        .Search(o => o.Field1, new List<string>() { "11" })
                        .Search(o => o.Field2, new List<string>() { "*" })
                        .Search(o => o.Field2, new List<string>() { "31" });
                    var l5 = await q5.ToListAsync();
                    AssertQueryResults(arr, l5);
                    var indexes5 = store.Maintenance.Send(new GetStatisticsOperation()).Indexes;
                    Assert.Equal(1, indexes5.Length);
                    Assert.Equal("Auto/TestObjs/BySearch(Field1)AndSearch(Field2)", indexes5[0].Name);
                }
            }
        }

        void AssertQueryResults(TestObj[] expectedResults, List<TestObj> actualResults)
        {
            Assert.NotNull(actualResults);
            Assert.Equal(expectedResults.Length, actualResults.Count);
            for (int i = 0; i < expectedResults.Length; i++)
            {
                Assert.True(actualResults.Contains(expectedResults[i]));
            }
        }

        class TestObj
        {
            public string Field1 { get; set; }
            public string Field2 { get; set; }

            protected bool Equals(TestObj other)
            {
                return GetHashCode()==other.GetHashCode();
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj))
                    return false;
                if (obj.GetType() != GetType())
                    return false;
                return Equals((TestObj)obj);
            }

            public override int GetHashCode()
            {
                return $"{Field1}{Field2}{Field2}{Field1}{Field1}{Field1}{Field2}{Field2}".GetHashCode();
            }
        }
    }
}
