using System.Linq;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4877 : RavenTest
    {
        public class User
        {
            public string Name { get; set; }
            public string[] Pets { get; set; }
        }

        public class Users_ByNameAndPets : AbstractIndexCreationTask<User>
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
            var coolPets = new[] { "Brian", "Garfield", "Nemo"};
            var petsToSearchFor = new[] {"Brian"};

            var sadUser = new User {Name = "Michael", Pets = noPets };
            var happyUser = new User { Name = "Maxim", Pets = coolPets };
            using (var store = NewDocumentStore())
            {
                new Users_ByNameAndPets().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(sadUser);
                    session.Store(happyUser);

                    session.SaveChanges();
                    WaitForIndexing(store);

                    var result = session.Query<User, Users_ByNameAndPets>()
                        .Where(user => user.Pets.ContainsAny(petsToSearchFor) || user.Name == "Michael")
                        .ToList();
                    Assert.Equal(result.Count, 2);

                    // By definition, en empty set is a subset of an any set (including empty set). 
                    // Following that logic --> An empty array contains an empty array
                    result = session.Query<User, Users_ByNameAndPets>()
                        .Where(user => user.Pets.ContainsAny(noPets) || user.Name == "Moshe")
                        .ToList();
                    Assert.Equal(result.Count, 2);
                }
            }
        }
    }
}
