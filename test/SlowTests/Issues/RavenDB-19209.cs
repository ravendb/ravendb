using System;
using System.Linq;
using FastTests;
using Newtonsoft.Json.Serialization;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using SlowTests.MailingList;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19209 : RavenTestBase
{
    public RavenDB_19209(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void FacetByFieldDocumentQuery()
    {
        using var store = GetSampleDatabase();
        {
            using var session = store.OpenSession();
            var q = session.Advanced.DocumentQuery<Workspace, CamelCaseIndex>().AggregateBy(builder => builder.ByField(i => i.CamelCase));
            Assert.Equal("from index 'CamelCaseIndex' select facet(camelCase)", q.ToString());
            var aggregations = q.Execute();
            Assert.Equal(1, aggregations.Count);
            Assert.Equal("camelCase", aggregations.First().Key);
        }
    }
    
    [Fact]
    public void NumericalFacetViaDocumentQuery()
    {
        using var store = GetSampleDatabase();
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

    [Fact]
    public void NumericalFacet()
    {
        using var store = GetSampleDatabase();
        using (var session = store.OpenSession())
        {
            var q = session.Query<Workspace, CamelCaseIndex>().AggregateBy(builder => builder.ByRanges(i => i.NumericalValue > 0, workspace => workspace.NumericalValue < 10 && workspace.NumericalValue > 0));
            Assert.Equal("from index 'CamelCaseIndex' select facet(numericalValue > $p0, numericalValue > $p1 and numericalValue < $p2)", q.ToString());
            var aggregations = q.Execute();
            Assert.Equal(1, aggregations.Count);
            Assert.Equal("numericalValue", aggregations.First().Key);
        }
    }


    [Fact]
    public void CamelCaseInStaticIndexes()
    {
        using var store = GetSampleDatabase();
        
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
            Map = workspaces => workspaces.Select(i => new { Domain = i.Domain, CamelCase = i.CamelCase, NumericalValue = i.NumericalValue });
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

    [Fact]
    public void ShouldGetResultOnQuery()
    {
            using var store = GetSampleDatabase();

            using (var session = store.OpenSession())
            {
                var q = session.Query<Workspace>().Where(ws => ws.Domain == "Encom");
                Console.WriteLine(q.ToString());
                Assert.NotNull(q.FirstOrDefault());
            }
        
    }

    private IDocumentStore GetSampleDatabase()
    {
        var store = GetDocumentStore(new Options
        {
            ModifyDocumentStore = documentStore =>
            {
                documentStore.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                {
                    CustomizeJsonSerializer = (serializer) =>
                    {
                        serializer.ContractResolver = new CamelCasePropertyNamesContractResolver();
                    }
                };
                documentStore.Conventions.PropertyNameConverter = mi => $"{char.ToLower(mi.Name[0])}{mi.Name[1..]}";
            }
        });
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
}
