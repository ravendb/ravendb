using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_16045 : RavenTestBase
{
    public RavenDB_16045(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenFact(RavenTestCategory.Indexes)]
    public void QueryRelatedDocumentTest()
    {
        using var store = GetDocumentStore();

        var index = new TestDocDataIndex();
        
        index.Execute(store);

        using (var session = store.OpenSession())
        {
            var d1 = new Dto() { Quantity = 101 };

            session.Store(d1);
            
            var dr1 = new DtoWithReference() { ReferenceId = d1.Id, Quantity = 101 };
            
            session.Store(dr1);
            
            session.SaveChanges();
            
            Indexes.WaitForIndexing(store);
            
            WaitForUserToContinueTheTest(store);

            var queryResults = session.Query<TestDocDataIndex.IndexResult, TestDocDataIndex>().ProjectInto<TestDocDataIndex.IndexResult>().ToList();
            
            Assert.False(queryResults.First().HasQuantityChanged);
            Assert.False(queryResults.First().LeftIsGreater);
            Assert.False(queryResults.First().RightIsGreater);

            Assert.True(queryResults.First().EqualQuantity);
            Assert.True(queryResults.First().LeftIsGte);
            Assert.True(queryResults.First().RightIsGte);
        }
    }
    
    private class Dto
    {
        public string Id { get; set; }
        public decimal Quantity { get; set; }
    }

    private class DtoWithReference
    {
        public string Id { get; set; }
        public string ReferenceId { get; set; }
        public decimal Quantity { get; set; }
    }
    
    private class TestDocDataIndex : AbstractIndexCreationTask<DtoWithReference>
    {
        public class IndexResult
        {
            public string Id { get; set; }
            public bool HasQuantityChanged { get; set; }
            public bool EqualQuantity { get; set; }
            public bool LeftIsGreater { get; set; }
            public bool RightIsGreater { get; set; }
            public bool LeftIsGte { get; set; }
            public bool RightIsGte { get; set; }
        }
        
        public TestDocDataIndex()
        {
            Map = dtosWithReference => from dtoWithReference in dtosWithReference
                let dto = LoadDocument<Dto>(dtoWithReference.ReferenceId)
                select new IndexResult()
                {
                    HasQuantityChanged = dtoWithReference.Quantity != dto.Quantity,
                    EqualQuantity = dtoWithReference.Quantity == dto.Quantity,
                    LeftIsGreater = dtoWithReference.Quantity > dto.Quantity,
                    RightIsGreater = dtoWithReference.Quantity < dto.Quantity,
                    LeftIsGte = dtoWithReference.Quantity >= dto.Quantity,
                    RightIsGte = dtoWithReference.Quantity <= dto.Quantity
                };
            
            StoreAllFields(FieldStorage.Yes);
        }
    }
}
