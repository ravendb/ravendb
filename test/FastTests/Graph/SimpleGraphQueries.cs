using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Documents;
using Xunit;

namespace FastTests.Graph
{
    public class SimpleGraphQueries : RavenTestBase
    {
        public class Dog
        {
            public string Id;
            public string Name;
            public string[] Likes;
            public string[] Dislikes;
        }

        private void CreateData(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var arava = new Dog { Name = "Arava" };
                var oscar = new Dog { Name = "Oscar" };
                var pheobe = new Dog { Name = "Pheobe" };

                session.Store(arava);
                session.Store(oscar);
                session.Store(pheobe);

                arava.Likes = new[] { oscar.Id };
                arava.Dislikes = new[] { pheobe.Id };

                oscar.Likes = new[] { oscar.Id, pheobe.Id };

                pheobe.Likes = new[] { oscar.Id };
                pheobe.Dislikes = new[] { arava.Id };

                session.SaveChanges();
            }
        }

        [Fact]
        public void FindFriendlies()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);
                using (var session = store.OpenSession())
                {
                   var friends = session.Advanced.RawQuery<dynamic>(@"
match (fst:Dogs)-[:Likes]->(snd:Dogs)
")
                        .ToList();
                }
            }
        }
    }
}
