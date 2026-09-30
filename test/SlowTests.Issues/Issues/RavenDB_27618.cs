using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27618 : RavenTestBase
{
    public RavenDB_27618(ITestOutputHelper output) : base(output)
    {
    }

    private class Item
    {
        public string Id { get; set; }
        public int V { get; set; }
        public List<string> Tags { get; set; }
        public Nested Nested { get; set; }
        public string CompanyId { get; set; }
    }

    private class Nested
    {
        public int A { get; set; }
    }

    private class Company
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class Projection
    {
        public int V { get; set; }
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.JavaScript)]
    [InlineData("ObjectPattern", "map('Items', u => { const { V } = u; return { V }; })", "where V = 5")]
    [InlineData("ArrayPattern", "map('Items', u => { const [T] = u.Tags; return { T }; })", "where T = 't'")]
    [InlineData("CatchPattern", "map('Items', u => { try { return { V: u.V }; } catch ({ message }) { return { V: 0 }; } })", "where V = 5")]
    [InlineData("ForInPattern", "map('Items', u => { let K = null; for (const [c] in u.Nested) K = c; return { K }; })", "where K = 'A'")]
    [InlineData("ForInMember", "map('Items', u => { const o = {}; for (o.k in u.Nested) {} return { K: o.k }; })", "where K = 'A'")]
    [InlineData("AssignmentPattern", "map('Items', u => { let V; ({ V } = u); return { V }; })", "where V = 5")]
    public void DestructuringInJsMap(string name, string map, string where) => AssertIndexReturnsItem(name, map, where, additionalSource: null);

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.JavaScript)]
    [InlineData("TemplateDefault", "map('Items', (u, s = `${u.V}`) => ({ V: u.V }))", null)]
    [InlineData("OptionalChainDefault", "map('Items', (u, a = u?.V) => ({ V: u.V }))", null)]
    [InlineData("SpreadDefault", "map('Items', (u, o = { ...u }) => ({ V: u.V }))", null)]
    [InlineData("SourceSpreadDefault", "map('Items', u => ({ V: u.V, A: withDefault().a }))", "function withDefault(o = { ...{ a: 1 } }) { return o; }")]
    public void ParameterDefaultsInJsMap(string name, string map, string additionalSource) => AssertIndexReturnsItem(name, map, "where V = 5", additionalSource);

    private void AssertIndexReturnsItem(string name, string map, string where, string additionalSource)
    {
        using var store = GetDocumentStore();
        var definition = new IndexDefinition { Name = $"Items/{name}", Maps = { map } };
        if (additionalSource != null)
            definition.AdditionalSources = new Dictionary<string, string> { ["Helpers"] = additionalSource };
        store.Maintenance.Send(new PutIndexesOperation(definition));
        using (var session = store.OpenSession())
        {
            session.Store(new Item { V = 5, Tags = new List<string> { "t" }, Nested = new Nested { A = 7 } }, "items/1");
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);
        using (var session = store.OpenSession())
        {
            var ids = session.Advanced.RawQuery<Item>($"from index 'Items/{name}' {where}").ToList().Select(x => x.Id);
            Assert.Equal(new[] { "items/1" }, ids);
        }
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.JavaScript)]
    public void LoadInPatternDefaultTracksTheReference()
    {
        using var store = GetDocumentStore();
        store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
        {
            Name = "Items/PatternDefaultLoad", Maps = { "map('Items', u => { const { N = load(u.CompanyId, 'Companies').Name } = {}; return { N }; })" }
        }));
        using (var session = store.OpenSession())
        {
            session.Store(new Company { Name = "acme" }, "companies/1");
            session.Store(new Item { CompanyId = "companies/1" }, "items/1");
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);
        using (var session = store.OpenSession())
        {
            session.Load<Company>("companies/1").Name = "bolt";
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);
        using (var session = store.OpenSession())
        {
            var ids = session.Advanced.RawQuery<Item>("from index 'Items/PatternDefaultLoad' where N = 'bolt'").ToList().Select(x => x.Id);
            Assert.Equal(new[] { "items/1" }, ids);
        }
    }

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.JavaScript)]
    public void DestructuringInJsProjection()
    {
        using var store = GetDocumentStore();
        using (var session = store.OpenSession())
        {
            session.Store(new Item { V = 5 }, "items/1");
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            var result = session.Advanced.RawQuery<Projection>("declare function f(u) { const { V } = u; return { V }; } from Items as u select f(u)").Single();
            Assert.Equal(5, result.V);
        }
    }
}
