using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22516 : RavenTestBase
{
    public RavenDB_22516(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TestMultipleOrdering(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var bar1 = new Bar() { Foo = new Foo(){ BarBool = true, BarShort = 14, BarLong = 21 } };
                var bar2 = new Bar() { Foo = new Foo(){ BarBool = true, BarShort = 12, BarLong = 21 } };
                
                session.Store(bar1);
                session.Store(bar2);
                
                session.SaveChanges();
                
                var result = session.Query<Bar>()
                    .OrderByDescending(dto => dto.Foo.BarBool)
                    .ThenByDescending(dto => dto.Foo.BarShort)
                    .ToList();

                Assert.Equal(bar1.Id, result[0].Id);
                Assert.Equal(bar2.Id, result[1].Id);
                
                var bar3 = new Bar() { Foo = new Foo(){ BarBool = true, BarShort = 14, BarLong = 37 } };
                
                session.Store(bar3);
                
                session.SaveChanges();
                
                result = session.Query<Bar>()
                    .OrderByDescending(dto => dto.Foo.BarBool)
                    .ThenByDescending(dto => dto.Foo.BarShort)
                    .ThenByDescending(dto => dto.Foo.BarLong)
                    .ToList();
                
                Assert.Equal(bar3.Id, result[0].Id);
                Assert.Equal(bar1.Id, result[1].Id);
                Assert.Equal(bar2.Id, result[2].Id);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TestEntryComparerByLong(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var dto1 = new Dto { FirstLong = 21, SecondLong = 37 };
                var dto2 = new Dto { FirstLong = 21, SecondLong = 10 };
                var dto3 = new Dto { FirstLong = 21, SecondLong = 21 };
                
                session.Store(dto1);
                session.Store(dto2);
                session.Store(dto3);
                
                session.SaveChanges();
                
                var result = session.Query<Dto>()
                    .OrderByDescending(dto => dto.FirstLong)
                    .ThenByDescending(dto => dto.SecondLong)
                    .ToList();
                
                Assert.Equal(dto1.Id, result[0].Id);
                Assert.Equal(dto3.Id, result[1].Id);
                Assert.Equal(dto2.Id, result[2].Id);
                
                result = session.Query<Dto>()
                    .OrderByDescending(dto => dto.FirstLong)
                    .ThenBy(dto => dto.SecondLong)
                    .ToList();
                
                Assert.Equal(dto2.Id, result[0].Id);
                Assert.Equal(dto3.Id, result[1].Id);
                Assert.Equal(dto1.Id, result[2].Id);
            }
        }
    }

    private class Bar
    {
        public string Id { get; set; }
        public Foo Foo { get; set; } = null!;
    }
    
    private class Foo
    {
        public string Id { get; set; }
        public short BarShort { get; set; }
        public bool BarBool { get; set; }
        public long BarLong { get; set; }
    }

    private class Dto
    {
        public string Id { get; set; }
        public long FirstLong { get; set; }
        public long SecondLong { get; set; }
    }
}
