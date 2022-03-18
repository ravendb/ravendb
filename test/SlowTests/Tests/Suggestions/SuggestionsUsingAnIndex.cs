//-----------------------------------------------------------------------
// <copyright file="Suggestions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Suggestions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Suggestions
{
    public class SuggestionsUsingAnIndex : RavenTestBase
    {
        public SuggestionsUsingAnIndex(ITestOutputHelper output) : base(output)
        {
        }

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

        [Fact]
        public void ExactMatch()
        {
            using (var documentStore = GetDocumentStore())
            {
                new DefaultSuggestionIndex().Execute(documentStore);
                //documentStore.ExecuteIndex(new DefaultSuggestionIndex());

                using (var s = documentStore.OpenSession())
                {
                    s.Store(new User { Name = "Ayende" });
                    s.Store(new User { Name = "Oren" });
                    s.SaveChanges();
                }

                Indexes.WaitForIndexing(documentStore);

                using (var session = documentStore.OpenSession())
                {
                    var suggestionQueryResult = session.Query<User>("DefaultSuggestionIndex")
                        .SuggestUsing(x => x.ByField(y => y.Name, "Owen").WithOptions(new SuggestionOptions
                        {
                            PageSize = 10
                        }))
                        .Execute();

                    Assert.Equal(1, suggestionQueryResult["Name"].Suggestions.Count);
                    Assert.Equal("oren", suggestionQueryResult["Name"].Suggestions[0]);
                }
            }
        }

        [Fact]
        public void UsingLinq()
        {
            using (var documentStore = GetDocumentStore())
            {
                new DefaultSuggestionIndex().Execute(documentStore);
                // documentStore.ExecuteIndex();

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
            using (var documentStore = GetDocumentStore())
            {
                new DefaultSuggestionIndex().Execute(documentStore);
                //documentStore.ExecuteIndex(new DefaultSuggestionIndex());

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
            using (var documentStore = GetDocumentStore())
            {
                new DefaultSuggestionIndex().Execute(documentStore);
                //documentStore.ExecuteIndex(new DefaultSuggestionIndex());

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
            using (var documentStore = GetDocumentStore())
            {
                new SuggestionIndex().Execute(documentStore);
                //documentStore.ExecuteIndex(new SuggestionIndex());

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
        public void WithTypo()
        {
            using (var documentStore = GetDocumentStore())
            {
                new SuggestionIndex().Execute(documentStore);
                //documentStore.ExecuteIndex(new SuggestionIndex());

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
                        .SuggestUsing(x => x.ByField(y => y.Name, "Oern").WithOptions(new SuggestionOptions
                        {
                            PageSize = 10,
                            Accuracy = 0.1f,
                            Distance = StringDistanceTypes.NGram
                        }))
                        .Execute();

                    Assert.Equal(1, suggestionQueryResult["Name"].Suggestions.Count);
                    Assert.Equal("oren", suggestionQueryResult["Name"].Suggestions[0]);
                }
            }
        }
    }
}
