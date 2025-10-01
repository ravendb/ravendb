using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20079 : RavenTestBase
{
    public RavenDB_20079(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenFact(RavenTestCategory.Indexes)]
    public void CanCreateIndexWithDefaultDateTime()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var e1 = new Entity();
                
                session.Store(e1);
                session.SaveChanges();
                
                var index = new TestIndex();
                index.Execute(store);
                
                Indexes.WaitForIndexing(store);

                var res = session.Query<TestIndex.Result, TestIndex>().Where(x => x.DefaultDateTime == default).ProjectInto<TestIndex.Result>().ToList();
                
                Assert.Equal(1, res.Count);
            }
        }
    }
    
    private class TestIndex : AbstractIndexCreationTask<Entity, TestIndex.Result>
    {
        public class Result
        {
            public DateTime DefaultDateTime { get; set; }
            public TimeOnly DefaultTimeOnly { get; set; }
            public DateOnly DefaultDateOnly { get; set; }
            public DateTimeOffset DefaultDateTimeOffset { get; set; }
            public TimeSpan DefaultTimeSpan { get; set; }
        }
        public TestIndex()
        {
            Map = collection => from c in collection
                select new Result
                {
                    DefaultDateTime = default,
                    DefaultTimeOnly = default,
                    DefaultDateOnly = default,
                    DefaultDateTimeOffset = default,
                    DefaultTimeSpan = default
                };
            
            StoreAllFields(FieldStorage.Yes);
        }
    }
}
