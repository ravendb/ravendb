using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;


namespace SlowTests.Issues
{
    public class RavenDB_17543 : RavenTestBase
    {
        public RavenDB_17543(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public Task CanCompoundOrderingInMapReduceResult()
        {
            using var store = GetDocumentStore();
            using (var session = store.OpenSession())
            {
                session.Store(new TestDoc(){Id = "a",Group = "dummy",Value1 = 10,Value2 = 30});
                session.Store(new TestDoc(){Id = "b",Group = "dummy",Value1 = 10,Value2 = 40});
                session.Store(new TestDoc(){Id = "c",Group = "dummy",Value1 = 20,Value2 = 30});
                session.Store(new TestDoc(){Id = "d",Group = "dummy",Value1 = 20,Value2 = 40});
                session.SaveChanges();
            }

            new TestIndexOrderByThenBy().Execute(store);
            Indexes.WaitForIndexing(store);
            using (var session = store.OpenSession())
            {
                var ravenQueryable = session.Query<Result, TestIndexOrderByThenBy>().ToList();

                var single = ravenQueryable.Single();
                
                Assert.True(single.AscAsc.OrderBy(doc => doc.Value1).ThenBy(doc => doc.Value2).SequenceEqual(single.AscAsc));
                Assert.True(single.AscDesc.OrderBy(doc => doc.Value1).ThenByDescending(doc => doc.Value2).SequenceEqual(single.AscDesc));
                Assert.True(single.DescAsc.OrderByDescending(doc => doc.Value1).ThenBy(doc => doc.Value2).SequenceEqual(single.DescAsc));
                Assert.True(single.DescDesc.OrderByDescending(doc => doc.Value1).ThenByDescending(doc => doc.Value2).SequenceEqual(single.DescDesc));

            }

            return null;
        }
        
        private class TestDoc
        {
            public string Id { get; set; }
            public string Group { get; set; }
            public int Value1 { get; set; }
            public int Value2 { get; set; }

            public override string ToString()
            {
                return $"Id: {Id}, Group: {Group}, Value1: {Value1}, Value2: {Value2}";
            }
        }
    
        private class Result
        {
            public string Group { get; set; }
            public TestDoc[] AscAsc { get; set; }
            public TestDoc[] AscDesc { get; set; }
            public TestDoc[] DescAsc { get; set; }
            public TestDoc[] DescDesc { get; set; }
        }

        private class TestIndexOrderByThenBy : AbstractIndexCreationTask<TestDoc, Result>
        {
            public TestIndexOrderByThenBy()
            {
                Map = docs => docs.Select(doc => new Result { Group = doc.Group, DescAsc = new[] { doc }, DescDesc = new[] { doc }, AscAsc = new[] { doc }, AscDesc = new[] { doc }  });
                Reduce = results => results
                    .GroupBy(result => new { result.Group })
                    .Select(result => new Result()
                    {
                        Group = result.Key.Group,
                        AscAsc = result.SelectMany(result1 => result1.AscAsc).OrderBy(doc => doc.Value1).ThenBy(doc => doc.Value2).ToArray(),
                        AscDesc = result.SelectMany(result1 => result1.AscAsc).OrderBy(doc => doc.Value1).ThenByDescending(doc => doc.Value2).ToArray(),
                        DescAsc = result.SelectMany(result1 => result1.AscAsc).OrderByDescending(doc => doc.Value1).ThenBy(doc => doc.Value2).ToArray(),
                        DescDesc = result.SelectMany(result1 => result1.AscAsc).OrderByDescending(doc => doc.Value1).ThenByDescending(doc => doc.Value2).ToArray()
                    });
            }
        }
    }
}
