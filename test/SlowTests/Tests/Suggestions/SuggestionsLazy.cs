//-----------------------------------------------------------------------
// <copyright file="SuggestionsLazy.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using SlowTests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Tests.Suggestions
{
    public class SuggestionsLazy : RavenTestBase
    {
        [Fact]
        public void UsingLinq()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition
                {
                    Name = "Test",
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

                using (var s = store.OpenSession())
                {
                    var oldRequests = s.Advanced.NumberOfRequests;

                    var suggestionQueryResult = s.Query<User>("test")
                        .SuggestUsing(x => x.ByField(y => y.Name, "Owen"))
                        .ExecuteLazy();

                    Assert.Equal(oldRequests, s.Advanced.NumberOfRequests);
                    Assert.Equal(1, suggestionQueryResult.Value["Name"].Suggestions.Count);
                    Assert.Equal("oren", suggestionQueryResult.Value["Name"].Suggestions[0]);

                    Assert.Equal(oldRequests + 1, s.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void LazyAsync()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition
                {
                    Name = "Test",
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

                using (var s = store.OpenAsyncSession())
                {
                    var oldRequests = s.Advanced.NumberOfRequests;

                    var suggestionQueryResult = s.Query<User>("test")
                        .SuggestUsing(x => x.ByField(y => y.Name, "Owen"))
                        .ExecuteLazyAsync();

                    Assert.Equal(oldRequests, s.Advanced.NumberOfRequests);
                    Assert.Equal(1, suggestionQueryResult.Value.Result["Name"].Suggestions.Count);
                    Assert.Equal("oren", suggestionQueryResult.Value.Result["Name"].Suggestions[0]);

                    Assert.Equal(oldRequests + 1, s.Advanced.NumberOfRequests);
                }
            }
        }
    }
}
