using System.Linq;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron.Data.PostingLists;
using Voron.Util;
using Xunit;

namespace FastTests.Corax;

public class RavenDB_23695 : RavenTestBase
{
    public RavenDB_23695(ITestOutputHelper output) : base(output)
    {
    }

    [RavenMultiplatformFact(RavenTestCategory.Vector, RavenArchitecture.AllX64)]
    public void CanPerformNoopUpdateOfVector()
    {
        using var store = GetDocumentStore();
        string id = "Products/1-A";
        using (var session = store.OpenSession())
        {
            session.Store(new Product() { Name = "abc", Category = "first"}, id);
            session.SaveChanges();
            _ = session.Advanced.DocumentQuery<Product>().WhereEquals(s => s.Category, "first").OrElse().VectorSearch(f => f.WithText(s => s.Name), v => v.ByText("abc"))
                .ToList();
            Indexes.WaitForIndexing(store);
        }

        using (var session = store.OpenSession())
        {
            var p = session.Load<Product>(id);
            p.Category = "updated"; 
            session.SaveChanges();
            Indexes.WaitForIndexing(store);


            var result = session.Advanced.DocumentQuery<Product>().WhereEquals(s => s.Category, "first").OrElse().VectorSearch(f => f.WithText(s => s.Name), v => v.ByText("abc"))
                .ToList();
            Assert.Equal(1, result.Count);
            
        }
    }
    
    [RavenMultiplatformTheory(RavenTestCategory.Vector, RavenArchitecture.AllX64)]
    [InlineData(4, 5)]
    [InlineData(5, 4)]
    public void CanPerformListDegradationAndUpgrade(int from, int to)
    {
        using var store = GetDocumentStore();
        string id = "Products/1-A";
        using (var session = store.OpenSession())
        {
            session.Store(new Product() { Name = "abc", Category = "first", Names = Enumerable.Range(0, from).Select(_ => "abc").ToArray()}, id);
            session.SaveChanges();
            _ = session.Advanced.DocumentQuery<Product>().WhereEquals(s => s.Category, "first").OrElse().VectorSearch(f => f.WithText(s => s.Names), v => v.ByText("abc"))
                .ToList();
            Indexes.WaitForIndexing(store);
        }

        using (var session = store.OpenSession())
        {
            var p = session.Load<Product>(id);
            p.Names =  Enumerable.Range(0, to).Select(_ => "abc").ToArray();
            session.SaveChanges();
            Indexes.WaitForIndexing(store);


            var result = session.Advanced.DocumentQuery<Product>().WhereEquals(s => s.Category, "first").OrElse().VectorSearch(f => f.WithText(s => s.Names), v => v.ByText("abc"))
                .ToList();
            Assert.Equal(1, result.Count);
            
        }
    }

    [RavenFact(RavenTestCategory.Core)]
    public void CanProperlyPrepareListForPostingList_Update()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var clb = new ContextBoundNativeList<long>(ctx);
        clb.Add(4);
        clb.Add(4);
        clb.Add(5);
        
        //The method is used to prepare final list that should be stored in the posting list.
        //It should:
        // - Filter updates (sequence: [ X, X, X | 1] ) => [X] )
        // - Filter removals (sequence: [X, X | 1] => [] )
        // - Addition (sequence: [X] => [X] )
        PostingList.SortModificationsAndRemoveDuplicates(ref clb);
        Assert.Equal(1, clb.Count);
        Assert.Equal(4, clb[0]);
    }
    
    [RavenFact(RavenTestCategory.Core)]
    public void CanProperlyPrepareListForPostingList_Removal()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var clb = new ContextBoundNativeList<long>(ctx);
        clb.Add(4);
        clb.Add(5);
        clb.Add(16);
        
        PostingList.SortModificationsAndRemoveDuplicates(ref clb);
        Assert.Equal(1, clb.Count);
        Assert.Equal(16, clb[0]);
    }
    
    [RavenFact(RavenTestCategory.Core)]
    public void CanProperlyPrepareListForPostingList_Insert()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var clb = new ContextBoundNativeList<long>(ctx);
        clb.Add(4);
        clb.Add(8);
        clb.Add(16);
        
        PostingList.SortModificationsAndRemoveDuplicates(ref clb);
        Assert.Equal(3, clb.Count);
        Assert.Equal(4, clb[0]);
        Assert.Equal(8, clb[1]);
        Assert.Equal(16, clb[2]);
    }

    private class Product
    {
        public string Name { get; set; }
        public string Category { get; set; }
        public string[] Names { get; set; }
    }
}
