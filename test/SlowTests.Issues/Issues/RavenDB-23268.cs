using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23268 : RavenTestBase
{
    public RavenDB_23268(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public void TestCase()
    {
        const int count = 1;
        
        using (var store = GetDocumentStore())
        {
            var testIndex = new TestIndex();
            
            testIndex.Execute(store);
            
            using (var bulk = store.BulkInsert())
            {
                for (int i = 0; i < count; i++)
                {
                    var guid = Guid.NewGuid().ToString();
                    var testOjb = new TestOjb { Id = guid, Prop = guid };
                    bulk.Store(testOjb);
                }
            }
            
            Indexes.WaitForIndexing(store);
            
            var resetIndexOperation = new ResetIndexOperation(testIndex.IndexName, IndexResetMode.SideBySide);
            
            var exception = Assert.Throws<RavenException>(() => store.Maintenance.Send(resetIndexOperation));

            Assert.Contains("Side by side index reset is not supported for map-reduce indexes with output reduce to collection.", exception.Message);
            Assert.Equal(typeof(NotSupportedException), exception.InnerException?.GetType());
        }
    }
    
    private class TestOjb
    {
        public string Id { get; set; }
        public string Prop { get; set; }
    }
    
    private class TestIndex : AbstractIndexCreationTask<TestOjb, TestIndex.Result>
    {
        public class Result
        {
            public string IndexProp { get; set; }
            public int Count { get; set; }
        }
        public TestIndex()
        {
            Map = users => from user in users
                select new Result 
                {
                    IndexProp = user.Prop,
                    Count = 1
                };

            Reduce = objs => 
                from obj in objs 
                group obj by obj.IndexProp 
                into g 
                select new Result
                {
                    IndexProp = g.Key, 
                    Count = g.Sum(x => x.Count)
                };
            
            OutputReduceToCollection = "OutputCollection";
        }
    }
}
