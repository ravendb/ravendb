//-----------------------------------------------------------------------
// <copyright file="AnalyzerPerField.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Lucene.Net.Analysis;
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class AnalyzerPerField : RavenTest
	{
		[Fact]
		public void CanUseAnalyzerPerField()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("Movies",
												new IndexDefinition
												{
													Map = "from movie in docs.Movies select new { movie.Name, movie.Tagline}",
													Analyzers =
				                                		{
				                                			{"Name", typeof (SimpleAnalyzer).FullName},
				                                			{"Tagline", typeof (StopAnalyzer).FullName}
				                                		}
												});

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
					var movies = s.Advanced.LuceneQuery<Movie>("Movies")
						.Where("Name:DOLLY")
						.WaitForNonStaleResults()
						.ToList();

					Assert.Equal(1, movies.Count);

					movies = s.Advanced.LuceneQuery<Movie>("Movies")
						.Where("Tagline:she's")
						.WaitForNonStaleResults()
						.ToList();

					Assert.Equal(1, movies.Count);
				}
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
