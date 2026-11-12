using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_27834 : RavenTestBase
{
    public RavenDB_27834(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.ClientApi)]
    public void APredicateOnATerminalOperatorMustApplyToTheWholeOrOfTheSource()
    {
        using var store = GetDocumentStore();

        using (var session = store.OpenSession())
        {
            session.Store(new Item { Name = "a" }, "items/1");
            session.Store(new Item { Name = "b" }, "items/2");
            session.SaveChanges();
        }

        using var s = store.OpenSession();

        IRavenQueryable<Item> Filtered() => s.Query<Item>()
            .Customize(x => x.WaitForNonStaleResults())
            .Where(x => x.Name == "a" || x.Name == "c");

        Assert.Null(Filtered().FirstOrDefault(x => x.Id == "items/2"));
        Assert.Null(Filtered().SingleOrDefault(x => x.Id == "items/2"));
        Assert.Throws<InvalidOperationException>(() => Filtered().First(x => x.Id == "items/2"));
        Assert.Throws<InvalidOperationException>(() => Filtered().Single(x => x.Id == "items/2"));
        Assert.Equal(0, Filtered().Count(x => x.Id == "items/2"));
        Assert.Equal(0L, Filtered().LongCount(x => x.Id == "items/2"));
        Assert.False(Filtered().Any(x => x.Id == "items/2"));
    }

    private class Item
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}
