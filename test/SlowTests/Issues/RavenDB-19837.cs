using Raven.Client.Documents.Linq;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Xunit;
using Xunit.Abstractions;
using Newtonsoft.Json.Serialization;
using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;

namespace SlowTests.Issues;

public class RavenDB_19837 : RavenTestBase
{
    [Fact]
    public void AnonymousProjectionWithCamelCaseSerialization()
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
                documentStore.Conventions.PropertyNameConverter = mi => $"{Char.ToLower(mi.Name[0])}{mi.Name.Substring(1)}";
            }
        });
        var index = new PersonIndex();
        index.Execute(store);

        using (var session = store.OpenSession())
        {
            session.Store(new OriginPerson() {FirstName = "Maciej", LastName = "Jan"});
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var q = session.Query<IndexPersonMap, PersonIndex>().Select(i => new {FullName = i.FullName}).Single();
            Assert.Equal("Maciej Jan", q.FullName);
        }
    }

    public RavenDB_19837(ITestOutputHelper output) : base(output)
    {
    }

    private class OriginPerson
    {
        public string FirstName { get; set; }
        public string LastName { get; set; }
    }

    private class IndexPersonMap
    {
        public string FirstName { get; set; }
        public string LastName { get; set; }

        public string FullName { get; set; }
    }

    private class PersonIndex : AbstractIndexCreationTask<OriginPerson>
    {
        public PersonIndex()
        {
            Map = peoples => from person in peoples
                select new IndexPersonMap() {FirstName = person.FirstName, LastName = person.LastName, FullName = person.FirstName + " " + person.LastName};

            Store("fullName", FieldStorage.Yes);
        }
    }
}
