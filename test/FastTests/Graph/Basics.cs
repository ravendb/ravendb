using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents;
using Xunit;

namespace FastTests.Graph
{
    public class Basics : RavenTestBase
    {
        public class Movie
        {
            public string Name { get; set; }
        }

        public class Genre
        {
            public string Name { get; set; }
        }

        public class User
        {
            public string Name { get;set; }
            public int Age { get; set; }
        }

        [Fact]
        public void Can_add_edges_between_documents()
        {
            using (var store = new DocumentStore
            {
                Database   = "FooBar",
                Urls = new []{"http://live-test.ravendb.net"}
            })
            {
                store.Initialize();
                using (var session = store.OpenSession())
                {
                    var m1 = new Movie
                    {
                        Name = "Star Wars Episode 1"
                    };

                    var scifi = new Genre
                    {
                        Name = "Sci-Fi"
                    };

                    var fantasy = new Genre
                    {
                        Name = "Fantasy"
                    };

                    var adventure = new Genre
                    {
                        Name = "Adventure"
                    };

                    session.Store(m1);
                    session.Store(scifi);
                    session.Store(fantasy);
                    session.Store(adventure);

                    session.AddEdgeBetween(m1,scifi,"HasGenre", new Dictionary<string, string>{ { "Weight", "0.3" } });
                    session.AddEdgeBetween(m1,fantasy,"HasGenre", new Dictionary<string, string>{ { "Weight", "0.6" } });
                    session.AddEdgeBetween(m1,adventure,"HasGenre", new Dictionary<string, string>{ { "Weight", "0.1" } });
                    
                    session.AddEdgeBetween(fantasy,scifi,"Related");

                    session.SaveChanges();
                }

            }
        }
    }
}
