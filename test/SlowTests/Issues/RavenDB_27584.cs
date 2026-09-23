using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_27584 : RavenTestBase
{
    public RavenDB_27584(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CreateFieldUnderANamedKey(Options options) => Assert.Equal(1, Count<Users_NamedKey>(options, "where Dyn = 5"));

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CreateFieldInAnAdditionalSourcesHelper(Options options) => Assert.Equal(1, Count<Users_AdditionalSourcesHelper>(options, "where Dyn = 5"));

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CreateFieldOnlyInTheReduce(Options options) => Assert.Equal(1, Count<Users_CreateFieldInReduce>(options, "where Dyn = 2"));

    // an index without createField must still reject an unknown field
    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void AnIndexWithoutCreateFieldStillRejectsAnUnknownField(Options options)
    {
        var e = Assert.ThrowsAny<Exception>(() => Count<Users_NoCreateField>(options, "where Dyn = 5"));
        Assert.Contains("is not indexed", e.Message);
    }

    private long Count<TIndex>(Options options, string where) where TIndex : IAbstractIndexCreationTask, new()
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
}
