//-----------------------------------------------------------------------
// <copyright file="Suggestions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries.Suggestions;
using SlowTests.Core.Utils.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Suggestions
{
    public class Suggestions : RavenTestBase
    {
        public Suggestions(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Email { get; set; }
        }

        public void Setup(IDocumentStore store)
        {
            store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition
            {
                Name = "test",
                Maps = { "from doc in docs.Users select new { doc.Name }" },
                Fields = new Dictionary<string, IndexFieldOptions>
                {
                    {
                        "Name",
                        new IndexFieldOptions { Suggestions = true }
                    }
                }
            }}));

            using (var s = store.OpenSession())
            {
                s.Store(new User { Name = "Ayende" });
                s.Store(new User { Name = "Oren" });
                s.Store(new User {Name = "John Steinbeck" });
                s.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
        }

        [Fact]
        public void ExactMatch()
        {
            using (var store = GetDocumentStore())
            {
                Setup(store);
                using (var session = store.OpenSession())
                {
                    var suggestionQueryResult = session.Query<User>("Test")
                        .SuggestUsing(x => x.ByField(y => y.Name, "Oren").WithOptions(new SuggestionOptions
                        {
                            PageSize = 10
                        }))
                        .Execute();

                    Assert.Equal(0, suggestionQueryResult["Name"].Suggestions.Count);
                }
            }
        }

        [Fact]
        public void UsingLinq()
        {
            using (var store = GetDocumentStore())
            {
                Setup(store);
                using (var s = store.OpenSession())
                {
                    var suggestionQueryResult = s.Query<User>("test")
                        .SuggestUsing(x => x.ByField(y => y.Name, "Owen"))
                        .Execute();

                    Assert.Equal(1, suggestionQueryResult["Name"].Suggestions.Count);
                    Assert.Equal("oren", suggestionQueryResult["Name"].Suggestions[0]);
                }
            }
        }

        [Fact]
        public void UsingLinq_with_typo_with_options_multiple_fields()
        {
            using (var store = GetDocumentStore())
            {
                Setup(store);
                using (var s = store.OpenSession())
                {
                    var suggestionQueryResult = s.Query<User>("test")
                        .SuggestUsing(x => x.ByField(y => y.Name, "Orin"))
                        .Execute();

                    Assert.Equal(1, suggestionQueryResult["Name"].Suggestions.Count);
                    Assert.Equal("oren", suggestionQueryResult["Name"].Suggestions[0]);
                }
            }
        }

        [Fact]
        public void UsingLinq_with_typo_multiple_fields_in_reverse_order()
        {
            using (var store = GetDocumentStore())
            {
                Setup(store);
                using (var session = store.OpenSession())
                {
                    var suggestionQueryResult = session.Query<User>("test")
                        .SuggestUsing(x => x.ByField(y => y.Name, "Orin"))
                        .Execute();

                    Assert.Equal(1, suggestionQueryResult["Name"].Suggestions.Count);
                    Assert.Equal("oren", suggestionQueryResult["Name"].Suggestions[0]);
                }
            }
        }

        [Fact]
        public void UsingLinq_WithOptions()
        {
            using (var store = GetDocumentStore())
            {
                Setup(store);
                using (var s = store.OpenSession())
                {
                    var suggestionQueryResult = s.Query<User>("test")
                        .SuggestUsing(x => x.ByField(y => y.Name, "Orin").WithOptions(new SuggestionOptions
                        {
                            Accuracy = 0.4f
                        }))
                        .Execute();

                    Assert.Equal(1, suggestionQueryResult["Name"].Suggestions.Count);
                    Assert.Equal("oren", suggestionQueryResult["Name"].Suggestions[0]);
                }
            }
        }

        [Fact]
        public void UsingLinq_Multiple_words()
        {
            using (var store = GetDocumentStore())
            {
                Setup(store);
                using (var s = store.OpenSession())
                {
                    var suggestionQueryResult = s.Query<User>("test")
                        .SuggestUsing(x => x.ByField(y => y.Name, "John Steinback").WithOptions(new SuggestionOptions
                        {
                            Accuracy = 0.5f,
                            Distance = StringDistanceTypes.Levenshtein
                        }))
                        .Execute();

                    Assert.Equal(1, suggestionQueryResult["Name"].Suggestions.Count);
                    Assert.Equal("john steinbeck", suggestionQueryResult["Name"].Suggestions[0]);
                }
            }
        }

        [Fact]
        public void WithTypo()
        {
            using (var store = GetDocumentStore())
            {
                Setup(store);
                using (var session = store.OpenSession())
                {
                    var suggestionQueryResult = session.Query<User>("Test")
                            .SuggestUsing(x => x.ByField(y => y.Name, "Oern").WithOptions(new SuggestionOptions
                            {
                                PageSize = 10,
                                Accuracy = 0.2f,
                                Distance = StringDistanceTypes.Levenshtein
                            }))
                            .Execute();

                    Assert.Equal(1, suggestionQueryResult["Name"].Suggestions.Count);
                    Assert.Equal("oren", suggestionQueryResult["Name"].Suggestions[0]);
                }
            }
        }

        [Fact]
        public void CanGetSuggestions()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Users_ByName();
                index.Execute(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new User { Name = "John Smith" }, "users/1");
                    s.Store(new User { Name = "Jack Johnson" }, "users/2");
                    s.Store(new User { Name = "Robery Jones" }, "users/3");
                    s.Store(new User { Name = "David Jones" }, "users/4");
                    s.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var suggestions = session.Query<User, Users_ByName>()
                            .SuggestUsing(x => x.ByField(y => y.Name, new[] { "johne", "davi" }).WithOptions(new SuggestionOptions
                            {
                                Accuracy = 0.4f,
                                PageSize = 5,
                                Distance = StringDistanceTypes.JaroWinkler,
                                SortMode = SuggestionSortMode.Popularity
                            }))
                            .Execute();

                    Assert.Equal(5, suggestions["Name"].Suggestions.Count);
                    Assert.Equal("john", suggestions["Name"].Suggestions[0]);
                    Assert.Equal("jones", suggestions["Name"].Suggestions[1]);
                    Assert.Equal("johnson", suggestions["Name"].Suggestions[2]);
                    Assert.Equal("david", suggestions["Name"].Suggestions[3]);
                    Assert.Equal("jack", suggestions["Name"].Suggestions[4]);
                }
            }
        }

        [Fact]
        public void CanGetResultAfterAddingADocument()
        {
            using (var store = GetDocumentStore())
            {
                new Users_ByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Jack Sparrow" }, "users/1");
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var suggestions = session.Query<User, Users_ByName>()
                        .SuggestUsing(x => x.ByField(y => y.Name, new[] { "jac" }).WithOptions(new SuggestionOptions
                        {
                            Accuracy = 0.4f,
                            PageSize = 5,
                            Distance = StringDistanceTypes.JaroWinkler,
                            SortMode = SuggestionSortMode.Popularity
                        }))
                        .Execute();

                    Assert.Equal(1, suggestions["Name"].Suggestions.Count);
                    Assert.Equal("jack", suggestions["Name"].Suggestions[0]);
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Will Turner" }, "users/2");
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var suggestions = session.Query<User, Users_ByName>()
                        .SuggestUsing(x => x.ByField(y => y.Name, new[] { "wil" }).WithOptions(new SuggestionOptions
                        {
                            Accuracy = 0.4f,
                            PageSize = 5,
                            Distance = StringDistanceTypes.JaroWinkler,
                            SortMode = SuggestionSortMode.Popularity
                        }))
                        .Execute();

                    Assert.Equal(1, suggestions["Name"].Suggestions.Count);
                    Assert.Equal("will", suggestions["Name"].Suggestions[0]);
                }
            }
        }
    }
}
