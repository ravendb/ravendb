using System;
using System.Linq;
using FastTests;
using Microsoft.AspNetCore.Identity;
using Newtonsoft.Json.Serialization;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19209 : RavenTestBase
{
    public RavenDB_19209(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void FacetByFieldDocumentQuery(Options options)
    {
        using var store = GetSampleDatabase(options);
        {
            using var session = store.OpenSession();
            var q = session.Advanced.DocumentQuery<Workspace, CamelCaseIndex>().AggregateBy(builder => builder.ByField(i => i.CamelCase));
            Assert.Equal("from index 'CamelCaseIndex' select facet(camelCase)", q.ToString());
            var aggregations = q.Execute();
            Assert.Equal(1, aggregations.Count);
            Assert.Equal("camelCase", aggregations.First().Key);
        }
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void NumericalFacetViaDocumentQuery(Options options)
    {
        using var store = GetSampleDatabase(options);
        {
            using var session = store.OpenSession();
            var q = session.Advanced.DocumentQuery<Workspace, CamelCaseIndex>().AggregateBy(builder =>
                builder.ByRanges(i => i.NumericalValue > 0, workspace => workspace.NumericalValue < 10 && workspace.NumericalValue > 0));
            Assert.Equal("from index 'CamelCaseIndex' select facet(numericalValue > $p0, numericalValue > $p1 and numericalValue < $p2)", q.ToString());
            var aggregations = q.Execute();
            Assert.Equal(1, aggregations.Count);
            Assert.Equal("numericalValue", aggregations.First().Key);
        }
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void NumericalFacet(Options options)
    {
        using var store = GetSampleDatabase(options);
        using (var session = store.OpenSession())
        {
            var q = session.Query<Workspace, CamelCaseIndex>().AggregateBy(builder =>
                builder.ByRanges(i => i.NumericalValue > 0, workspace => workspace.NumericalValue < 10 && workspace.NumericalValue > 0));
            Assert.Equal("from index 'CamelCaseIndex' select facet(numericalValue > $p0, numericalValue > $p1 and numericalValue < $p2)", q.ToString());
            var aggregations = q.Execute();
            Assert.Equal(1, aggregations.Count);
            Assert.Equal("numericalValue", aggregations.First().Key);
        }
    }


    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void CamelCaseInStaticIndexes(Options options)
    {
        using var store = GetSampleDatabase(options);

        using (var session = store.OpenSession())
        {
            var q = session.Query<Workspace, CamelCaseIndex>().AggregateBy(builder => builder.ByField(i => i.CamelCase));
            Assert.Equal("from index 'CamelCaseIndex' select facet(camelCase)", q.ToString());
            var aggregations = q.Execute();
            Assert.Equal(1, aggregations.Count);
            Assert.Equal("camelCase", aggregations.First().Key);
        }
    }

    private class CamelCaseIndex : AbstractIndexCreationTask<Workspace>
    {
        public CamelCaseIndex()
        {
            Map = workspaces => workspaces.Select(i => new {Domain = i.Domain, CamelCase = i.CamelCase, NumericalValue = i.NumericalValue});
        }
    }

    private class Workspace
    {
        public string Id { get; set; }

        //[JsonProperty("domain")]
        public string Domain { get; set; }

        public string CamelCase { get; set; }

        public int NumericalValue { get; set; }
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void ShouldGetResultOnQuery(Options options)
    {
        using var store = GetSampleDatabase(options);

        using (var session = store.OpenSession())
        {
            var q = session.Query<Workspace>().Where(ws => ws.Domain == "Encom");
            WaitForUserToContinueTheTest(store);
            Assert.NotNull(q.FirstOrDefault());
        }
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void ShouldGetUserIdentityResultOnQuery(Options options)
    {
        using (var store = CamelCaseStoreWithCustomConvention(options))
        {
            using (var session = store.OpenSession())
            {
                var user = new MyIdentityUser {UserName = "john"};

                session.Store(user, "users/1");
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var user = session.Query<MyIdentityUser>().FirstOrDefault(u => u.UserName == "john");
                Assert.NotNull(user);
            }
        }
    }

    private IDocumentStore CamelCaseStoreWithCustomConvention(Options options)
    {
        options.ModifyDocumentStore = ss =>
        {
            ss.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
            {
                CustomizeJsonSerializer = serializer =>
                {
                    serializer.ContractResolver = new CamelCasePropertyNamesContractResolver();
                }
            };
            ss.Conventions.PropertyNameConverter = mi => $"{Char.ToLower(mi.Name[0])}{mi.Name.Substring(1)}";
            ss.Conventions.ShouldApplyPropertyNameConverter = info => true;
        };

        return GetDocumentStore(options);
    }

    private IDocumentStore GetSampleDatabase(Options options)
    {
        options.ModifyDocumentStore = documentStore =>
        {
            documentStore.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
            {
                CustomizeJsonSerializer = (serializer) =>
                {
                    serializer.ContractResolver = new CamelCasePropertyNamesContractResolver();
                }
            };
            documentStore.Conventions.PropertyNameConverter = mi => $"{char.ToLower(mi.Name[0])}{mi.Name[1..]}";
        };

        var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            var workspace1 = new Workspace {Domain = "Encom", CamelCase = "SuperSecretTest", NumericalValue = 1};
            session.Store(workspace1, "workspaces/1");
            session.SaveChanges();
        }

        new CamelCaseIndex().Execute(store);
        Indexes.WaitForIndexing(store);
        return store;
    }

    private class MyIdentityUser : IdentityUser
    {
    }
}
