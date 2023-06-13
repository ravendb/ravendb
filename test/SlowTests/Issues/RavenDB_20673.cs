using FastTests;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_20673 : RavenTestBase
    {
        public RavenDB_20673(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private void Setup(IDocumentStore store)
        {
            using (var s = store.OpenSession())
            {
                s.Store(new User {Name = "dan"});
                s.Store(new User {Name = "daniel"});
                s.Store(new User {Name = "danielle"});
                s.SaveChanges();
            }
        }

        [Fact]
        public void CustomizeDisplayNameWithSpaces()
        {
            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    var suggestionQuery = s.Query<User>()
                        .SuggestUsing(builder => builder
                            .ByField(x => x.Name, "daniele")
                            .WithDisplayName("Customized name with spaces"));

                    var rql = suggestionQuery.ToString();
                    Assert.Contains("Customized name with spaces", rql);

                    var results = suggestionQuery.Execute();
                    Assert.Equal(2, results["Customized name with spaces"].Suggestions.Count);
                    Assert.Equal("danielle", results["Customized name with spaces"].Suggestions[0]);
                }
            }
        }

        [Fact]
        public void CustomizeDisplayNameWithOutSpaces()
        {
            using (var store = GetDocumentStore())
            {
                Setup(store);

                using (var s = store.OpenSession())
                {
                    var suggestionQuery = s.Query<User>()
                        .SuggestUsing(builder => builder
                            .ByField(x => x.Name, "daniele")
                            .WithDisplayName("CustomizedName"));

                    var rql = suggestionQuery.ToString();
                    Assert.Contains("CustomizedName", rql);

                    var results = suggestionQuery.Execute();
                    Assert.Equal(2, results["CustomizedName"].Suggestions.Count);
                    Assert.Equal("danielle", results["CustomizedName"].Suggestions[0]);
                }
            }
        }
    }
}
