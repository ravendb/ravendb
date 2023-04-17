using System;
using System.Linq;
using FastTests;
using Microsoft.AspNetCore.JsonPatch;
using Newtonsoft.Json.Serialization;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20283 : RavenTestBase
{
    public RavenDB_20283(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenFact(RavenTestCategory.Patching | RavenTestCategory.ClientApi)]
    public void ShouldPathDocumentWithCamelCase()
    {
        using (var store = GetCamelCaseDocStore())
        {
            using (var session = store.OpenSession())
            {
                var user = new MyUser {UserName = "john", Age = 10};
                session.Store(user, "users/1");
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var user = session.Query<MyUser>().Single();
                var patchesDocument = new JsonPatchDocument();
                patchesDocument.Add("/analytics", new {Visits = 32});
                session.Advanced.DocumentStore.Operations.Send(new JsonPatchOperation("users/1", patchesDocument));
                session.Advanced.Patch(user, x => x.Age, 21); // write test
                session.SaveChanges();
            }
            
            GetDocumentsCommand getDocsCommand = new GetDocumentsCommand(store.Conventions, "users/1", null, false);
            store.Commands().Execute(getDocsCommand);
            var res = getDocsCommand.Result.Results[0] as BlittableJsonReaderObject;
            Assert.Contains("\"visits\":32", res!.ToString());
            Assert.Contains("\"age\":21.0", res!.ToString());
        }
    }

    private DocumentStore GetCamelCaseDocStore()
    {
        return GetDocumentStore(new Options()
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
                documentStore.Conventions.ShouldApplyPropertyNameConverter = info => true;
            }
        });
    }


    private class MyUser
    {
        public string UserName { get; set; }
        
        public decimal Age { get; set; }
    }
}
