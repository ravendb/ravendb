using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27584 : RavenTestBase
{
    public RavenDB_27584(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task CreateFieldUnderANamedKey(Options options) => Assert.Equal(1, await Count<Users_NamedKey>(options, "where Dyn = 5", hasDynamicFields: true));

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task CreateFieldInABoostedDocument(Options options) => Assert.Equal(1, await Count<Users_BoostedDocument>(options, "where Dyn = 5", hasDynamicFields: true));

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task CreateFieldInAnAdditionalSourcesHelper(Options options) => Assert.Equal(1, await Count<Users_AdditionalSourcesHelper>(options, "where Dyn = 5", hasDynamicFields: true));

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task CreateFieldOnlyInTheReduce(Options options) => Assert.Equal(1, await Count<Users_CreateFieldInReduce>(options, "where Dyn = 2", hasDynamicFields: true));

    // an index without createField must still reject an unknown field
    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task AnIndexWithoutCreateFieldStillRejectsAnUnknownField(Options options)
    {
        var e = await Assert.ThrowsAnyAsync<RavenException>(() => Count<Users_NoCreateField>(options, "where Dyn = 5", hasDynamicFields: false));
        Assert.Contains("is not indexed", e.Message);
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task CreateFieldWithoutOptionsDoesNotSetTheFlag(Options options)
    {
        using var store = GetDocumentStore(options);

        var index = new Users_CreateFieldWithTwoArguments();
        await index.ExecuteAsync(store);

        var database = await GetDatabase(store.Database);
        Assert.False(database.IndexStore.GetIndex(index.IndexName).Definition.HasDynamicFields);
    }

    private async Task<long> Count<TIndex>(Options options, string where, bool hasDynamicFields) where TIndex : IAbstractIndexCreationTask, new()
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            session.Store(new User { Name = "a", V = 5 });
            session.Store(new User { Name = "a", V = 7 });
            session.SaveChanges();
        }

        var index = new TIndex();
        index.Execute(store);
        Indexes.WaitForIndexing(store);

        var database = await GetDatabase(store.Database);
        Assert.Equal(hasDynamicFields, database.IndexStore.GetIndex(index.IndexName).Definition.HasDynamicFields);

        using (var session = store.OpenSession())
        {
            session.Advanced.RawQuery<object>($"from index '{index.IndexName}' {where}")
                .Statistics(out var stats)
                .ToList();

            return stats.TotalResults;
        }
    }

    private class User
    {
        public string Id { get; set; }

        public string Name { get; set; }

        public int V { get; set; }
    }

    private class Users_NamedKey : AbstractJavaScriptIndexCreationTask
    {
        public Users_NamedKey()
        {
            Maps = new HashSet<string>
            {
                "map('Users', u => ({ Name: u.Name, Attr: createField('Dyn', u.V, { indexing: 'Exact', storage: false }) }))"
            };
        }
    }

    private class Users_BoostedDocument : AbstractJavaScriptIndexCreationTask
    {
        public Users_BoostedDocument()
        {
            Maps = new HashSet<string>
            {
                "map('Users', u => boost({ Name: u.Name, _: createField('Dyn', u.V, { indexing: 'Exact', storage: false }) }, 2))"
            };
        }
    }

    private class Users_AdditionalSourcesHelper : AbstractJavaScriptIndexCreationTask
    {
        public Users_AdditionalSourcesHelper()
        {
            Maps = new HashSet<string>
            {
                "map('Users', u => ({ Name: u.Name, Attr: dyn(u) }))"
            };
            AdditionalSources = new Dictionary<string, string>
            {
                ["helpers"] = "function dyn(u) { return createField('Dyn', u.V, { indexing: 'Exact', storage: false }); }"
            };
        }
    }

    // for-of and the template literal make sure the reduce is parsed without the collection visitor, which throws on them
    private class Users_CreateFieldInReduce : AbstractJavaScriptIndexCreationTask
    {
        public Users_CreateFieldInReduce()
        {
            Maps = new HashSet<string>
            {
                "map('Users', u => ({ Name: u.Name, Count: 1 }))"
            };
            Reduce = "groupBy(x => x.Name).aggregate(g => { let c = 0; for (const v of g.values) c += v.Count; " +
                     "return { Name: g.key, Count: c, Attr: createField(`Dyn`, c, { indexing: 'Exact', storage: false }) }; })";
        }
    }

    private class Users_NoCreateField : AbstractJavaScriptIndexCreationTask
    {
        public Users_NoCreateField()
        {
            Maps = new HashSet<string>
            {
                "map('Users', u => ({ Name: u.Name }))"
            };
        }
    }

    private class Users_CreateFieldWithTwoArguments : AbstractJavaScriptIndexCreationTask
    {
        public Users_CreateFieldWithTwoArguments()
        {
            Maps = new HashSet<string>
            {
                "map('Users', u => ({ Name: u.Name, Attr: createField('Dyn', u.V) }))"
            };
        }
    }
}
