//-----------------------------------------------------------------------
// <copyright file="SuggestionsLazy.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Linq;
using Raven.Tests.Bugs;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Suggestions
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public class SuggestionsLazyAsync : RavenTest
    {
        [Fact]
        public async Task UsingLinq()
        {
            using (GetNewServer())
            using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
            {
                store.DatabaseCommands.PutIndex("Test", new IndexDefinition
                {
                    Map = "from doc in docs select new { doc.Name }",
                    SuggestionsOptions = new HashSet<string> { "Name" }
                });
                using (var s = store.OpenSession())
                {
                    s.Store(new User { Name = "Ayende" });
                    s.Store(new User { Name = "Oren" });
                    s.SaveChanges();

                    s.Query<User>("Test").Customize(x => x.WaitForNonStaleResults()).ToList();
                }

                using (var s = store.OpenAsyncSession())
                {
                    var oldRequests = s.Advanced.NumberOfRequests;

                    var suggestionQueryResult = s.Query<User>("test")
                        .Where(x => x.Name == "Owen")
                        .SuggestLazyAsync();

                    Assert.Equal(oldRequests, s.Advanced.NumberOfRequests);
                    Assert.Equal(1, (await suggestionQueryResult.Value).Suggestions.Length);
                    Assert.Equal("oren", (await suggestionQueryResult.Value).Suggestions[0]);

                    Assert.Equal(oldRequests + 1, s.Advanced.NumberOfRequests);
                }
            }
        }
    }
}
