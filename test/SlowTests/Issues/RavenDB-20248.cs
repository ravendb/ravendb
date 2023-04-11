using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20248 : RavenTestBase
{
    public RavenDB_20248(ITestOutputHelper output) : base(output)
    {
    }
    
    [Fact]
    public void CanSortByNestedPropertyViaDocumentQuery()
    {
        using var store = GetDocumentStore();
        using (var session = store.OpenSession())
        {
            session.Store(new First() {Name = "Maciej", Inner = new Inner() {SortOrder = "aaaa"}});
            session.Store(new First() {Name = "macieJ", Inner = new Inner() {SortOrder = "bbbb"}});
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            var source = session.Advanced.DocumentQuery<First>()
                .WaitForNonStaleResults()
                .WhereEquals(i => i.Name, "maciej")
                .AddOrder(i => i.Inner.SortOrder, descending: true)
                .ToQueryable();

            var query =
                from e in source
                select new First {Name = e.Name + "Test", Inner = e.Inner};

            var queryResult = query.ToList();

            Assert.Equal(2, queryResult.Count);
            Assert.Equal(queryResult[0].Name, "macieJ" + "Test");
            Assert.Equal(queryResult[1].Name, "Maciej" + "Test");
        }
    }

    [Fact]
    public void TestFilterAliasing()
    {
        using var store = GetDocumentStore();
        using (var session = store.OpenSession())
        {
            session.Store(new First() {Name = "Name1", Inner = new Inner() {SortOrder = "order1"}});
            session.Store(new First() {Name = "Name2", Inner = new Inner() {SortOrder = "order2"}});
            session.Store(new First() {Name = "Name3", Inner = new Inner() {SortOrder = "order1"}});
            session.SaveChanges();
            
            var source = session.Advanced.DocumentQuery<First>()
                .Filter(i => i.Equals(a => a.Inner.SortOrder, "order1"))
                .ToQueryable();
            
            var query =
                from e in source
                select new First {Name = e.Name + "Test", Inner = e.Inner};
            
            var queryResult = query.ToList();
            
            Assert.Equal(2, queryResult.Count);
            Assert.Equal(queryResult[0].Name, "Name1" + "Test");
            Assert.Equal(queryResult[1].Name, "Name3" + "Test");
        }
    }
    
    [Fact]
    public void TestGroupByAliasing()
    {
        using var store = GetDocumentStore();
        using (var session = store.OpenSession())
        {
            session.Store(new First() {Name = "Name1", Inner = new Inner() {SortOrder = "order1"}});
            session.Store(new First() {Name = "Name2", Inner = new Inner() {SortOrder = "order2"}});
            session.Store(new First() {Name = "Name3", Inner = new Inner() {SortOrder = "order1"}});
            session.SaveChanges();
            
            var query = session.Query<First>()
                .GroupBy(x => new { Inner = x.Inner })
                .Select(x => new { Inner = x.Key.Inner, Count = x.Count() });
            
            var queryResult = query.ToList();
            
            Assert.Equal(2, queryResult.Count);
            Assert.Equal(2, queryResult[0].Count);
            Assert.Equal(1, queryResult[1].Count);
            Assert.Equal("order1", queryResult[0].Inner.SortOrder);
            Assert.Equal("order2", queryResult[1].Inner.SortOrder);
        }
    }
    
    private class First
    {
        public string Name { get; set; }
        public Inner Inner { get; set; }
    }

    private class Inner
    {
        public string SortOrder { get; set; }
    }
}
