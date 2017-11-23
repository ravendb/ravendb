//-----------------------------------------------------------------------
// <copyright file="Suggestions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries.Suggestion;
using SlowTests.Core.Utils.Indexes;
using Xunit;

namespace SlowTests.Tests.Suggestions
{
    public class Suggestions : RavenTestBase
    {
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
                s.SaveChanges();
            }

            WaitForIndexing(store);
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
                        .Where(x => x.Name == "Oren")
                        .Suggest(new SuggestionQuery
                        {
                            MaxSuggestions = 10
                        });

                    Assert.Equal(0, suggestionQueryResult.Suggestions.Length);
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
                        .Where(x => x.Name == "Owen")
                        .Suggest();

                    Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
                    Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
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
                        .Where(x => x.Name == "Orin")
                        .Where(x => x.Email == "whatever")
                        .Suggest(new SuggestionQuery { Field = "Name", Term = "Orin" });

                    Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
                    Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
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
                        .Where(x => x.Email == "whatever")
                        .Where(x => x.Name == "Orin")
                        .Suggest(new SuggestionQuery { Field = "Name", Term = "Orin" });

                    Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
                    Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
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
                        .Where(x => x.Name == "Orin")
                        .Suggest(new SuggestionQuery { Accuracy = 0.4f });

                    Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
                    Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
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
                        .Where(x => x.Name == "Oern") // intentional typo
                        .Suggest(new SuggestionQuery
                        {
                            MaxSuggestions = 10,
                            Accuracy = 0.2f,
                            Distance = StringDistanceTypes.Levenshtein
                        });

                    Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
                    Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
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

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var suggestions = session.Query<User, Users_ByName> ()
                                                    .Suggest(new SuggestionQuery
                                                    {
                                                        Field = "Name",
                                                        Term = "<<johne davi>>",
                                                        Accuracy = 0.4f,
                                                        MaxSuggestions = 5,
                                                        Distance = StringDistanceTypes.JaroWinkler,
                                                        Popularity = true,
                                                    });

                    Assert.Equal(5, suggestions.Suggestions.Length);
                    Assert.Equal("john", suggestions.Suggestions[0]);
                    Assert.Equal("jones", suggestions.Suggestions[1]);
                    Assert.Equal("johnson", suggestions.Suggestions[2]);
                    Assert.Equal("david", suggestions.Suggestions[3]);
                    Assert.Equal("jack", suggestions.Suggestions[4]);
                }
            }
        }
    }
}
