using System;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Database.Indexing;
using Xunit;
using Raven.Client.Document;

namespace Raven.Client.Tests.Bugs
{
	public class AutoCreateIndexes : BaseClientTest
	{
		[Fact]
		public void CanAutomaticallyCreateIndexes()
		{
			using (var store = NewDocumentStore())
			{
				IndexCreation.CreateIndexes(typeof(Movies_ByActor).Assembly, store);

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
					var movies = s.LuceneQuery<Movie>("Movies/ByActor")
						.Where("Name:Dolly")
						.WaitForNonStaleResults()
						.ToList();

					Assert.Equal(1, movies.Count);
				}
			}
		}

		public class Movies_ByActor : AbstractIndexCreationTask
		{
			public override IndexDefinition CreateIndexDefinition()
			{
				return new IndexDefinition<Movie>
				{
					Map = movies => from movie in movies
					                select new {movie.Name}
				}
				.ToIndexDefinition(DocumentStore.Conventions);
			}
		}

		public class Movie
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public string Tagline { get; set; }
		}
	}
}