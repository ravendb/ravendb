using System;
using System.Linq;
using System.Reflection;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Xunit.Abstractions;
using Xunit;
using Newtonsoft.Json.Serialization;
using Raven.Client.Documents.Commands;
using Sparrow.Json;

namespace FastTests.Issues
{
    public class RavenDB_20566 : RavenTestBase
    {
        public RavenDB_20566(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldGetUserResultOnQuery()
        {
            using (var store = GetDocumentStore(new Options()
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
                    documentStore.Conventions.FindIdentityProperty = ConventionsFindIdentityProperty;
                    documentStore.Conventions.FindIdentityPropertyNameFromCollectionName = s => "id";
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    var user = new MyUser
                    {
                        UserName = "john"
                    };

                    session.Store(user, "users/1");
                    session.SaveChanges();
                }

                WaitForUserToContinueTheTest(store);

                using (var session = store.OpenSession())
                {
                    var user = session.Query<MyUser>().Where(u => u.UserName == "john").FirstOrDefault();
                    Assert.NotNull(user);
                    Assert.NotNull(user.Id);

                    var command = new GetDocumentsCommand(store.Conventions, new[] { "users/1" }, null, false);
                    session.Advanced.RequestExecutor.Execute(command, session.Advanced.Context);
                    var blittableUser = (BlittableJsonReaderObject)command.Result.Results[0];
                    Assert.False(blittableUser.GetPropertyNames().Contains("id"));
                }
            }
        }

        private bool ConventionsFindIdentityProperty(MemberInfo info)
        {
            var x = $"{Char.ToLower(info.Name[0])}{info.Name.Substring(1)}";
            return x == "id";
        }

        public class MyUser : MyUserBaseClass { }
        public class MyUserBaseClass
        {
            public string Id { get; set; }
            public string UserName { get; set; }
        }
    }
}
