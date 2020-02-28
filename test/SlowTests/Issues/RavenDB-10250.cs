using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Session;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10250 : RavenTestBase
    {
        public RavenDB_10250(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
#pragma warning disable 169,649
            public string Name;
#pragma warning restore 169,649
        }

        [Fact]
        public void CanUseBeforeStoreToAddNewDocsToSaveChanges()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    var documentStore = (IDocumentStore)s;
                    documentStore.OnBeforeStore += (sender, args) =>
                    {
                        ((IDocumentSession)args.Session).Advanced.Defer(new PutCommandData(args.DocumentId + "/companion", null,
                            new DynamicJsonValue
                            {
                                ["@metadata"] = new DynamicJsonValue
                                {
                                    ["@collection"] = "Friends"
                                },
                                ["Name"] = args.Entity.ToString()
                            }));
                    };
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/fred");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var c = session.Load<dynamic>("users/fred/companion");
                    Assert.NotNull(c.Name);
                }
            }
        }
    }
}
