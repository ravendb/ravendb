using System;
using System.Collections.Generic;
using System.Linq;
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
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var movie = new Movie
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

                    session.Store(movie);
                    session.Store(scifi);
                    session.Store(fantasy);
                    session.Store(adventure);

                    session.Advanced.AddEdgeBetween(movie,scifi,"HasGenre", new Dictionary<string, string>{ { "Weight", "0.3" } });
                    session.Advanced.AddEdgeBetween(movie,fantasy,"HasGenre", new Dictionary<string, string>{ { "Weight", "0.6" } });
                    session.Advanced.AddEdgeBetween(movie,adventure,"HasGenre", new Dictionary<string, string>{ { "Weight", "0.1" } });
                    
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var movie = session.Query<Movie>().First();
                    var edges = session.Advanced.GetEdgesOf(movie);

                    Assert.Equal(3,edges.Count);
                    Assert.All(edges, e => Assert.Equal("HasGenre",e.EdgeType));
                }

            }
        }
    }
}
