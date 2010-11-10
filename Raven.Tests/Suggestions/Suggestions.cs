using System;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Tests;
using Raven.Client.Tests.Bugs;
using Raven.Database.Indexing;
using Xunit;
using System.Linq;

namespace Raven.Tests.Suggestions
{
    public class Suggestions : LocalClientTest, IDisposable
    {
        #region IDisposable Members

        public void Dispose()
        {
           
        }

        #endregion

        public Suggestions()
        {
            
        }

        protected DocumentStore DocumentStore { get; set; }

        [Fact]
        public void ExactMatch()
        {
            using(var store = NewDocumentStore())
            {
                store.DatabaseCommands.PutIndex("Test", new IndexDefinition
                {
                    Map = "from doc in docs select new { doc.Name }",
                    Indexes = {{"Name", FieldIndexing.Analyzed}}
                });
                using(var s = store.OpenSession())
                {
                    s.Store(new User{Name = "Ayende"});
                    s.Store(new User { Name = "Oren" });
                    s.SaveChanges();

                    s.Query<User>("Test").Customize(x => x.WaitForNonStaleResults()).ToList();
                }

                using (var s = store.OpenSession())
                {
                    var suggestionQueryResult = s.Advanced.DatabaseCommands.Suggest("Test",
                                                                                    new SuggestionQuery
                                                                                    {
                                                                                        Field = "Name",
                                                                                        Term = "Oren",
                                                                                        MaxSuggestions = 10
                                                                                    });

                    Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
                    Assert.Equal("Oren", suggestionQueryResult.Suggestions[0]);
                }
            }
        }
    }
}
