//-----------------------------------------------------------------------
// <copyright file="SuggestionsLazy.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Operations.Databases.Indexes;
using SlowTests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Tests.Suggestions
{
    public class SuggestionsLazy : RavenNewTestBase
    {
        [Fact(Skip = "Missing feature: Suggestions")]
        public void UsingLinq()
        {
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new PutIndexOperation("Test", new IndexDefinition
                {
                    Maps = { "from doc in docs.Users select new { doc.Name }" },
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        {
                            "Name",
                            new IndexFieldOptions { Suggestions = true }
                        }
                    }
                }));

                using (var s = store.OpenSession())
                {
                    s.Store(new User { Name = "Ayende" });
                    s.Store(new User { Name = "Oren" });
                    s.SaveChanges();

                    s.Query<User>("Test").Customize(x => x.WaitForNonStaleResults()).ToList();
                }

                using (var s = store.OpenSession())
                {
                    var oldRequests = s.Advanced.NumberOfRequests;

                    var suggestionQueryResult = s.Query<User>("test")
                        .Where(x => x.Name == "Owen")
                        .SuggestLazy();

                    Assert.Equal(oldRequests, s.Advanced.NumberOfRequests);
                    Assert.Equal(1, suggestionQueryResult.Value.Suggestions.Length);
                    Assert.Equal("oren", suggestionQueryResult.Value.Suggestions[0]);

                    Assert.Equal(oldRequests + 1, s.Advanced.NumberOfRequests);
                }
            }
        }
    }
}
