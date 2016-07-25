using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Indexing;
using Raven.Client.Linq;
using Xunit;

namespace Raven.SlowTests.Issues
{
    public class RavenDB_4877 : RavenTestBase
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
        public async void CanQueryWhenContainsAnyIsEmpty()
        {
            var noPets = new string[0];
            var coolPets = new[] { "Brian", "Garfield", "Nemo" };
            var petsToSearchFor = new[] { "Brian" };

            var sadUser = new User { Name = "Michael", Pets = noPets };
            var happyUser = new User { Name = "Maxim", Pets = coolPets };

            var store = await GetDocumentStore();

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

                result = session.Query<User, Users_ByNameAndPets>()
                    .Where(user => user.Name == "Moshe" || user.Pets.ContainsAny(noPets))
                    .ToList();
                Assert.Equal(result.Count, 2);
            }
            store.Dispose();
        }
    }
}