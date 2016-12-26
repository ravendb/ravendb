using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using FastTests;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;
using Raven.Client.Linq;
using Sparrow.Json;

namespace Raven.Tests.Issues
{
    public class RavenDB_72 : RavenTestBase
    {
        [Fact(Skip = "Wait for RavenDB-5958")]
        public async void CanWork()
        {
            using (var store = GetDocumentStore())
            {
                const string searchQuery = "Doe";

                // Scan for all indexes inside the ASSEMBLY.
                new Users_ByDisplayNameReversed().Execute(store);

                // Seed some fake data.
                CreateFakeData(store);
                var xx = new string(searchQuery.Reverse().ToArray());

                // Now lets do our query.
                using (IDocumentSession documentSession = store.OpenSession())
                {
                  
                    var query = documentSession
                        .Query<Users_ByDisplayNameReversed.Result, Users_ByDisplayNameReversed>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.DisplayNameReversed.StartsWith(xx))
                        .As<User>();

                    var users = query.ToList();

                    Assert.NotEmpty(users);
                }

                var stats = await store.AsyncDatabaseCommands.GetIndexErrorsAsync();   
                Assert.Equal(0,stats.Length);
            }
        }

        public class User
        {
            public string Id { get; set; }
            public string DisplayName { get; set; }
        }

        public class Users_ByDisplayNameReversed : AbstractIndexCreationTask<User, Users_ByDisplayNameReversed.Result>
        {
            public class Result
            {
                public string Id { get; set; }
                public string DisplayName { get; set; }
                public string DisplayNameReversed { get; set; }
            }

            public Users_ByDisplayNameReversed()
            {
                Map = users => from doc in users
                               select new
                              {
                                  Id = doc.Id,
                                  DisplayName = doc.DisplayName,
                                  DisplayNameReversed = doc.DisplayName.Reverse()
                              };

                //Index(x => x.DisplayNameReversed, FieldIndexing.NotAnalyzed);
            }

            
        }

        private static void CreateFakeData(IDocumentStore documentStore)
        {
            if (documentStore == null)
            {
                throw new ArgumentNullException("documentStore");
            }

            var users = new List<User>();
            users.AddRange(new[]
            {
                new User {Id = null, DisplayName = "Fred Smith"},
                new User {Id = null, DisplayName = "Jane Doe"},
                new User {Id = null, DisplayName = "John Doe"},
                new User {Id = null, DisplayName = "Pure Krome"},
                new User {Id = null, DisplayName = "Ayende Rahien"},
                new User {Id = null, DisplayName = "Itamar Syn-Hershko"},
                new User {Id = null, DisplayName = "Oren Eini"},
              //  new User {Id = null, DisplayName = null} // <--- Assume this is an option field....
            });
            using (IDocumentSession documentSession = documentStore.OpenSession())
            {
                foreach (User user in users)
                {
                    documentSession.Store(user);
                }

                documentSession.SaveChanges();
            }
        }
    }
}
