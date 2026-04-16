using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_22191 : RavenTestBase
    {
        public RavenDB_22191(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Id { get; set; }
            public string Company { get; set; }
        }

        private class Result
        {
            public bool IsActive1 { get; set; }
            public bool IsActive2 { get; set; }
            public bool IsActive3 { get; set; }
            public bool IsActive4 { get; set; }
            public bool IsActive5 { get; set; }
            public bool IsActive6 { get; set; }
            public bool IsActive7 { get; set; }
            public bool IsActive8 { get; set; }
            public bool IsActive9 { get; set; }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void BooleanOperatorsWithDynamicNullObjectShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                const string indexName = "Users/ByIsActive";

                var storedField = new IndexFieldOptions { Storage = FieldStorage.Yes };

                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = indexName,
                    Maps =
                    {
                        @"from doc in docs.Users
                          let loadedActive = (LoadDocument(doc.Company, ""Companies"")).IsActive
                          select new {
                              IsActive1 = loadedActive && true,
                              IsActive2 = loadedActive && false,
                              IsActive3 = loadedActive || true,
                              IsActive4 = loadedActive || false,
                              IsActive5 = false || loadedActive,
                              IsActive6 = true && loadedActive,
                              IsActive7 = loadedActive | true,
                              IsActive8 = loadedActive || ""hello"",
                              IsActive9 = loadedActive || loadedActive
                          }"
                    },
                    Fields =
                    {
                        [nameof(Result.IsActive1)] = storedField,
                        [nameof(Result.IsActive2)] = storedField,
                        [nameof(Result.IsActive3)] = storedField,
                        [nameof(Result.IsActive4)] = storedField,
                        [nameof(Result.IsActive5)] = storedField,
                        [nameof(Result.IsActive6)] = storedField,
                        [nameof(Result.IsActive7)] = storedField,
                        [nameof(Result.IsActive8)] = storedField,
                        [nameof(Result.IsActive9)] = storedField
                    }
                }));

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1-A");
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { indexName }));
                Assert.Empty(errors[0].Errors);

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<Result>(
                            $"from index '{indexName}' select IsActive1, IsActive2, IsActive3, IsActive4, IsActive5, IsActive6, IsActive7, IsActive8, IsActive9")
                        .Single();

                    Assert.False(result.IsActive1, "loadedActive && true");
                    Assert.False(result.IsActive2, "loadedActive && false");
                    Assert.True(result.IsActive3, "loadedActive || true");
                    Assert.False(result.IsActive4, "loadedActive || false");
                    Assert.False(result.IsActive5, "false || loadedActive");
                    Assert.False(result.IsActive6, "true && loadedActive");
                    Assert.True(result.IsActive7, "loadedActive | true");
                    Assert.True(result.IsActive8, "loadedActive || \"hello\"");
                    Assert.False(result.IsActive9, "loadedActive || loadedActive");
                }
            }
        }
    }
}
