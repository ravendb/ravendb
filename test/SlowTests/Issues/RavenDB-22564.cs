using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22564 : RavenTestBase
{
    public RavenDB_22564(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public void TestIndexWithRecursion()
    {
        using (var store = GetDocumentStore())
        {
            var index = new TestIndex();
            
            index.Execute(store);
            
            using (var session = store.OpenSession())
            {
                var o1 = new TestObj { Prop = "o1" };
                var o2 = new TestObj { Prop = "o2" };
                
                session.Store(o1);
                session.Store(o2);
        
                var o3 = new TestObj
                {
                    Prop = "o3",
                    Children = new []
                    {
                        o1.Id,
                        o2.Id
                    }
                };
                
                session.Store(o3);
                
                var o4 = new TestObj { Prop = "o4" };
                var o5 = new TestObj { Prop = "o5" };
                
                session.Store(o4);
                session.Store(o5);
                
                var o6 = new TestObj
                {
                    Prop = "o6",
                    Children = new []
                    {
                        o4.Id,
                        o5.Id
                    }
                };
                
                session.Store(o6);

                session.Store(new TestObj { Prop = "o7", Children = new[] { o3.Id, o6.Id } });
                session.SaveChanges();

                Indexes.WaitForIndexing(store);
            
                var result = session.Query<TestIndex.Result, TestIndex>()
                    .Where(x => x.Prop == "o7")
                    .Select(x => x.Count)
                    .ToList();
                
                Assert.Equal(7, result.First());
            }
        }
    }
    
    private class TestObj
    {
        public string Id { get; set; }
        public IEnumerable<string> Children { get; set; }
        public string Prop { get; set; }
    }

    private class TestIndex : AbstractIndexCreationTask<TestObj, TestIndex.Result>
    {
        public class Result
        {
            public string Prop { get; set; }
            public string Children { get; set; }
            public int Count { get; set; }
        }
        public TestIndex()
        {
            Map = objs => from o in objs
                let children = Recurse(o, x => LoadDocument<TestObj>(x.Children))
                select new Result
                {
                    Prop = o.Prop, 
                    Children = string.Join(", ", children.Where(x => x != null).Select(x => x.Prop)),
                    Count = children.Count(x => x != null)
                };
        
            StoreAllFields(FieldStorage.Yes);
        }
    }
}
