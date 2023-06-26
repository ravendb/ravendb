using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20424 : RavenTestBase
{
    public RavenDB_20424(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void CanProjectRelatedDocumentViaJS()
    {
        using var store = GetDocumentStore();

        using (var s = store.OpenSession())
        {
            s.Store(new Cat("cats/1", "Origin", null));
            s.Store(new Cat("cats/2", "Clone", "cats/1"));
            s.SaveChanges();
        }

        using (var s = store.OpenSession())
        {
            var p = s.Advanced.RawQuery<Cat>("from Cats as c where id() = 'cats/2' load c.Parent as p select p").Single();
            Assert.Equal("cats/1", p.Id);
            Assert.Equal("Origin", p.Name);
        }
        WaitForUserToContinueTheTest(store);
        using (var s = store.OpenSession())
        {
            var p = s.Advanced.RawQuery<Cat>(
                "declare function p(c) { return load(c.Parent); } " +
                "from Cats as c where id() = 'cats/2' select p(c)").Single();
            Assert.Equal("Origin", p.Name);
            Assert.Equal("cats/1", p.Id);
        }
    }

    private record Cat(string Id, string Name, string Parent);
}
