using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_9584 : RavenTestBase
    {
        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Company { get; set; }
        }

        private static void Setup(IDocumentStore store)
        {
            store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition
            {
                Name = "test",
                Maps = { "from doc in docs.Users select new { doc.Name, doc.Company }" },
                Fields = new Dictionary<string, IndexFieldOptions>
                {
                    {
                        "Name",
                        new IndexFieldOptions { Suggestions = true }
                    },
                    {
                        "Company",
                        new IndexFieldOptions { Suggestions = true }
                    }
                }
            }}));

            using (var s = store.OpenSession())
            {
                s.Store(new User { Name = "Ayende", Company = "Hibernating" });
                s.Store(new User { Name = "Oren", Company = "HR" });
                s.Store(new User { Name = "John Steinbeck", Company = "Unknown" });
                s.SaveChanges();
            }

            WaitForIndexing(store);
        }

        [Fact]
        public async Task CanChainSuggestions()
        {
            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    var suggestionQueryResult = s.Query<User>("test")
                        .SuggestUsing(x => x.ByField(y => y.Name, "Owen"))
                        .AndSuggestUsing(x => x.ByField(y => y.Company, "Hiberanting"))
                        .Execute();

                    Assert.Equal(1, suggestionQueryResult["Name"].Suggestions.Count);
                    Assert.Equal("oren", suggestionQueryResult["Name"].Suggestions[0]);

                    Assert.Equal(1, suggestionQueryResult["Company"].Suggestions.Count);
                    Assert.Equal("hibernating", suggestionQueryResult["Company"].Suggestions[0]);
                }

                using (var s = store.OpenSession())
                {
                    var suggestionQueryResult = s.Advanced.DocumentQuery<User>("test")
                        .SuggestUsing(x => x.ByField(y => y.Name, "Owen"))
                        .AndSuggestUsing(x => x.ByField(y => y.Company, "Hiberanting"))
                        .Execute();

                    Assert.Equal(1, suggestionQueryResult["Name"].Suggestions.Count);
                    Assert.Equal("oren", suggestionQueryResult["Name"].Suggestions[0]);

                    Assert.Equal(1, suggestionQueryResult["Company"].Suggestions.Count);
                    Assert.Equal("hibernating", suggestionQueryResult["Company"].Suggestions[0]);
                }

                using (var s = store.OpenAsyncSession())
                {
                    var suggestionQueryResult = await s.Advanced.AsyncDocumentQuery<User>("test")
                        .SuggestUsing(x => x.ByField(y => y.Name, "Owen"))
                        .AndSuggestUsing(x => x.ByField(y => y.Company, "Hiberanting"))
                        .ExecuteAsync();

                    Assert.Equal(1, suggestionQueryResult["Name"].Suggestions.Count);
                    Assert.Equal("oren", suggestionQueryResult["Name"].Suggestions[0]);

                    Assert.Equal(1, suggestionQueryResult["Company"].Suggestions.Count);
                    Assert.Equal("hibernating", suggestionQueryResult["Company"].Suggestions[0]);
                }
            }
        }

        [Fact]
        public void CanUseAliasInSuggestions()
        {
            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    var suggestionQueryResult = s.Query<User>("test")
                        .SuggestUsing(x => x
                            .ByField(y => y.Name, "Owen")
                            .WithDisplayName("NewName"))
                        .Execute();

                    Assert.Equal(1, suggestionQueryResult["NewName"].Suggestions.Count);
                    Assert.Equal("oren", suggestionQueryResult["NewName"].Suggestions[0]);
                }
            }
        }

        [Fact]
        public void CanUseAliasInSuggestionsWithAutoIndex()
        {
            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    var suggestionQueryResult = s.Query<User>()
                        .SuggestUsing(x => x
                            .ByField(y => y.Name, "Owen")
                            .WithDisplayName("NewName"))
                        .Execute();

                    Assert.Equal(1, suggestionQueryResult["NewName"].Suggestions.Count);
                    Assert.Equal("oren", suggestionQueryResult["NewName"].Suggestions[0]);
                }
            }
        }
    }
}
