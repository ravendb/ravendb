//-----------------------------------------------------------------------
// <copyright file="AutoCreateIndexes.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Bugs
{
    public class AutoCreateIndexes : RavenTestBase
    {
        [Fact]
        public void CanAutomaticallyCreateIndexes()
        {
            using (var store = GetDocumentStore())
            {
                new Movies_ByActor().Execute(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new Movie
                    {
                        Name = "Hello Dolly",
                        Tagline = "She's a jolly good"
                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var movies = s.Advanced.DocumentQuery<Movie>("Movies/ByActor")
                        .WhereLucene("Name", "Dolly")
                        .WaitForNonStaleResults()
                        .ToList();

                    Assert.Equal(1, movies.Count);
                }
            }
        }

        private class Movies_ByActor : AbstractIndexCreationTask<Movie>
        {
            public Movies_ByActor()
            {
                Map = movies => from movie in movies
                                select new {movie.Name};
                Index(x=>x.Name, FieldIndexing.Analyzed);
            }
        }

        private class Movie
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Tagline { get; set; }
        }
    }
}
