using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_4877 : RavenTestBase
    {
        public RavenDB_4877(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Name { get; set; }
            public string[] Pets { get; set; }
        }

        private class Users_ByNameAndPets : AbstractIndexCreationTask<User>
        {
            public Users_ByNameAndPets()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Name,
                                   user.Pets
                               };
            }
        }

        [Fact]
        public void CanQueryWhenContainsAnyIsEmpty()
        {
            var noPets = new string[0];
            var coolPets = new[] { "Brian", "Garfield", "Nemo" };
            var petsToSearchFor = new[] { "Brian" };

            var sadUser = new User { Name = "Michael", Pets = noPets };
            var happyUser = new User { Name = "Maxim", Pets = coolPets };
            using (var store = GetDocumentStore())
            {
                new Users_ByNameAndPets().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(sadUser);
                    session.Store(happyUser);

                    session.SaveChanges();
                    Indexes.WaitForIndexing(store);

                    var result = session.Query<User, Users_ByNameAndPets>()
                        .Where(user => user.Pets.ContainsAny(petsToSearchFor) || user.Name == "Michael")
                        .ToList();
                    Assert.Equal(result.Count, 2);

                    // https://github.com/apache/commons-lang/blob/dd2394323b441e7a22d3c85ce751b619918ee161/src/main/java/org/apache/commons/lang3/StringUtils.java#L2152
                    result = session.Query<User, Users_ByNameAndPets>()
                        .Where(user => user.Pets.ContainsAny(noPets) || user.Name == "Moshe")
                        .ToList();
                    Assert.Equal(0, result.Count);
                }
            }
        }
    }
}
