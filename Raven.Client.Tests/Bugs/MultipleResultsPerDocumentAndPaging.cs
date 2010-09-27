using System.Linq;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Client.Tests.Bugs
{
	public class MultipleResultsPerDocumentAndPaging: LocalClientTest
	{
		[Fact]
		public void WhenOutputingMultipleResultsPerDocAndPagingWillGetCorrectSize()
		{
			using(var store = NewDocumentStore())
			{

				store.DatabaseCommands.PutIndex("Movies/ByActor", new IndexDefinition
				{
					Map = @"
from movie in docs.Movies
from actor in movie.Actors
select new { Actor = actor }",
                    Indexes = {{"Actor", FieldIndexing.Analyzed}}
				});

				using(var s1 = store.OpenSession())
				{
					s1.Store(
						new Movie
						{
							Name = "Inception",
							Actors = new[] { "Leonardo DiCaprio", "Joseph Gordon-Levitt", "Ellen Page", "Tom Hardy", "James Bond", "Shames Bond" }
						});
					s1.Store(
						new Movie
						{
							Name = "The Sorcerer's Apprentice",
							Actors = new[] { "Nicolas Cage", "Jay Baruchel", "Alfred Molina", "Teresa Palmer", "James Bond", "Shames Bond" }
						}); 
					s1.SaveChanges();
				}

				using (var s2 = store.OpenSession())
				{
					var movies = s2.LuceneQuery<Movie>("Movies/ByActor")
						.WhereContains("Actor", "Bond")
						.Take(2)
						.WaitForNonStaleResults()
						.ToList();

					Assert.Equal(2, movies.Count);
				}
			}
		}

		public class Movie
		{
			public string Id { get; set; }
			public string Name { get; set; }

			public string[] Actors { get; set; }
		}

	}
}