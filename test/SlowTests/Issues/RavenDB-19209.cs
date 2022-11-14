using System;
using System.Linq;
using FastTests;
using Newtonsoft.Json.Serialization;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19209 : RavenTestBase
{
    public RavenDB_19209(ITestOutputHelper output) : base(output)
    {
    }


    [Fact]
    public void CamelCaseInStaticIndexes()
    {
        using var store = GetDocumentStore(new Options
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
            var workspace1 = new Workspace {Domain = "Encom", CamelCase = "SuperSecretTest"};
            session.Store(workspace1, "workspaces/1");
            session.SaveChanges();
        }
        new CamelCaseIndex().Execute(store);

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
            Map = workspaces => workspaces.Select(i => new { Domain = i.Domain, CamelCase = i.CamelCase });
        }
    }
    
    private class Workspace
    {
        public string Id { get; set; }

        //[JsonProperty("domain")]
        public string Domain { get; set; }

        public string CamelCase { get; set; }
    }

    [Fact]
    public void ShouldGetResultOnQuery()
    {
        using (var store = GetDocumentStore(new Options
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
               }))
        {
            using (var session = store.OpenSession())
            {
                var workspace1 = new Workspace { Domain = "Encom" };
                session.Store(workspace1, "workspaces/1");
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var q = session.Query<Workspace>().Where(ws => ws.Domain == "Encom");
                Console.WriteLine(q.ToString());
                Assert.NotNull(q.FirstOrDefault());
            }
        }
    }
}
