//-----------------------------------------------------------------------
// <copyright file="Suggestions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.NewClient.Client.Indexes;
using Xunit;

namespace SlowTests.Tests.Suggestions
{
    public class SuggestionsUsingAnIndex : RavenNewTestBase
    {
        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Email { get; set; }
        }

        private class DefaultSuggestionIndex : AbstractIndexCreationTask<User>
        {
            public DefaultSuggestionIndex()
            {
                Map = users => from user in users
                               select new { user.Name };

                Suggestion(user => user.Name);
            }
        }

        private class SuggestionIndex : AbstractIndexCreationTask<User>
        {
            public SuggestionIndex()
            {
                Map = users => from user in users
                               select new { user.Name };

                Suggestion(user => user.Name);
            }
        }

        [Fact(Skip = "Missing feature: Suggestions")]
        public void ExactMatch()
        {
            using (var documentStore = GetDocumentStore())
            {
                documentStore.ExecuteIndex(new DefaultSuggestionIndex());

                using (var s = documentStore.OpenSession())
                {
                    s.Store(new User { Name = "Ayende" });
                    s.Store(new User { Name = "Oren" });
                    s.SaveChanges();

                    s.Query<User, DefaultSuggestionIndex>().Customize(x => x.WaitForNonStaleResults()).ToList();
                }

                using (var session = documentStore.OpenSession())
                {
                    var suggestionQueryResult = session.Query<User>("DefaultSuggestionIndex")
                        .Where(x => x.Name == "Owen")
                        .Suggest(new SuggestionQuery
                        {
                            MaxSuggestions = 10
                        });

                    Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
                    Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
                }
            }
        }

        [Fact(Skip = "Missing feature: Suggestions")]
        public void UsingLinq()
        {
            using (var documentStore = GetDocumentStore())
            {
                documentStore.ExecuteIndex(new DefaultSuggestionIndex());

                using (var s = documentStore.OpenSession())
                {
                    s.Store(new User { Name = "Ayende" });
                    s.Store(new User { Name = "Oren" });
                    s.SaveChanges();

                    s.Query<User, DefaultSuggestionIndex>().Customize(x => x.WaitForNonStaleResults()).ToList();
                }

                using (var session = documentStore.OpenSession())
                {
                    var suggestionQueryResult = session.Query<User, DefaultSuggestionIndex>()
                                                       .Where(x => x.Name == "Owen")
                                                       .Suggest();

                    Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
                    Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
                }
            }
        }

        [Fact(Skip = "Missing feature: Suggestions")]
        public void UsingLinq_with_typo_with_options_multiple_fields()
        {
            using (var documentStore = GetDocumentStore())
            {
                documentStore.ExecuteIndex(new DefaultSuggestionIndex());

                using (var s = documentStore.OpenSession())
                {
                    s.Store(new User { Name = "Ayende" });
                    s.Store(new User { Name = "Oren" });
                    s.SaveChanges();

                    s.Query<User, DefaultSuggestionIndex>().Customize(x => x.WaitForNonStaleResults()).ToList();
                }

                using (var session = documentStore.OpenSession())
                {
                    var suggestionQueryResult = session.Query<User, DefaultSuggestionIndex>()
                                                       .Where(x => x.Name == "Orin")
                                                       .Where(x => x.Email == "whatever")
                                                       .Suggest(new SuggestionQuery { Field = "Name", Term = "Orin" });

                    Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
                    Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
                }
            }
        }

        [Fact(Skip = "Missing feature: Suggestions")]
        public void UsingLinq_with_typo_multiple_fields_in_reverse_order()
        {
            using (var documentStore = GetDocumentStore())
            {
                documentStore.ExecuteIndex(new DefaultSuggestionIndex());

                using (var s = documentStore.OpenSession())
                {
                    s.Store(new User { Name = "Ayende" });
                    s.Store(new User { Name = "Oren" });
                    s.SaveChanges();

                    s.Query<User, DefaultSuggestionIndex>().Customize(x => x.WaitForNonStaleResults()).ToList();
                }

                using (var session = documentStore.OpenSession())
                {
                    var suggestionQueryResult = session.Query<User, DefaultSuggestionIndex>()
                                                       .Where(x => x.Email == "whatever")
                                                       .Where(x => x.Name == "Orin")
                                                       .Suggest(new SuggestionQuery { Field = "Name", Term = "Orin" });

                    Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
                    Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
                }
            }
        }

        [Fact(Skip = "Missing feature: Suggestions")]
        public void UsingLinq_WithOptions()
        {
            using (var documentStore = GetDocumentStore())
            {
                documentStore.ExecuteIndex(new SuggestionIndex());

                using (var s = documentStore.OpenSession())
                {
                    s.Store(new User { Name = "Ayende" });
                    s.Store(new User { Name = "Oren" });
                    s.SaveChanges();

                    s.Query<User, SuggestionIndex>().Customize(x => x.WaitForNonStaleResults()).ToList();
                }

                using (var session = documentStore.OpenSession())
                {
                    var suggestionQueryResult = session.Query<User, SuggestionIndex>()
                                                       .Where(x => x.Name == "Orin")
                                                       .Suggest(new SuggestionQuery { Accuracy = 0.4f });

                    Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
                    Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
                }
            }
        }

        [Fact(Skip = "Missing feature: Suggestions")]
        public void WithTypo()
        {
            using (var documentStore = GetDocumentStore())
            {
                documentStore.ExecuteIndex(new SuggestionIndex());

                using (var s = documentStore.OpenSession())
                {
                    s.Store(new User { Name = "Ayende" });
                    s.Store(new User { Name = "Oren" });
                    s.SaveChanges();

                    s.Query<User, SuggestionIndex>().Customize(x => x.WaitForNonStaleResults()).ToList();
                }

                using (var session = documentStore.OpenSession())
                {
                    var suggestionQueryResult = session.Query<User>("SuggestionIndex")
                        .Where(x => x.Name == "Oern") // intentional typo
                        .Suggest(new SuggestionQuery
                        {
                            MaxSuggestions = 10,
                            Accuracy = 0.1f,
                            Distance = StringDistanceTypes.NGram
                        });

                    Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
                    Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
                }
            }
        }
    }
}
