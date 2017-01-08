using System.Linq;
using Raven.Client.Linq;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB6055 : RavenTestBase
    {
        public class User
        {
            public string FirstName;
            public string LastName;
        }

        [Fact]
        public void CreatingNewAutoIndexWillDeleteSmallerOnes()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Query<User>()
                        .Where(x => x.FirstName == "Alex")
                        .ToList();

                    var indexes = store.DatabaseCommands.GetIndexes(0,25);
                    Assert.Equal(1, indexes.Length);
                    Assert.Equal("Auto/Users/ByFirstName", indexes[0].Name);
                }

                using (var session = store.OpenSession())
                {
                    session.Query<User>()
                        .Where(x => x.LastName == "Smith")
                        .ToList();

                    var indexes = store.DatabaseCommands.GetIndexes(0, 25);
                    Assert.Equal("Auto/Users/ByFirstNameAndLastName", indexes[0].Name);
                }
            }
        }
    }
}