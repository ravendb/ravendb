using System;
using System.Collections.Generic;
using System.Text;
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
            using (var store = GetDocumentStore())
            {
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

                    session.AddEdgeBetween(m1,scifi,"HasGenre", new { Weight = 0.3 });
                    session.AddEdgeBetween(m1,fantasy,"HasGenre", new { Weight = 0.6 });
                    session.AddEdgeBetween(m1,adventure,"HasGenre", new { Weight = 0.1 });

                    session.SaveChanges();
                }

                WaitForUserToContinueTheTest(store);
            }
        }
    }
}
