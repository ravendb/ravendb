using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Lucene.Net.Analysis;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Bundles.MoreLikeThis;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class MoreLikeThisEvaluation : RavenTestBase
	{
		private readonly IDocumentStore _store;

		public MoreLikeThisEvaluation()
		{
			_store = NewDocumentStore();
			_store.Initialize();
		}

		public override void Dispose()
		{
			_store.Initialize();
			base.Dispose();
		}

		[Fact]
		public void ShouldMatchTwoMoviesWithSameCast()
		{
			string id;
			using (var session = _store.OpenSession())
			{
				new MovieIndex().Execute(_store);
				var list = GetMovieList();
				list.ForEach(session.Store);
				session.SaveChanges();
				id = session.Advanced.GetDocumentId(list.First());
				WaitForIndexing(_store);
			}

			using (var session = _store.OpenSession())
			{
				var list = session
					.Advanced
					.MoreLikeThis<Movie, MovieIndex>(new MoreLikeThisQuery
					{
						DocumentId = id,
						Fields = new[] { "Cast" },
						MinimumTermFrequency = 1,
						MinimumDocumentFrequency = 2
					});

				Assert.NotEmpty(list);
			}
		}
		private static List<Movie> GetMovieList()
		{
			return new List<Movie> {
                                       new Movie
                                           {
                                               Title = "Star Wars Episode IV: A New Hope",
                                               Cast = new[]
                                                          {
                                                              "Mark Hamill", 
                                                              "Harrison Ford", 
                                                              "Carrie Fisher"
                                                          }
                                           },
                                       new Movie
                                           {
                                               Title = "Star Wars Episode V: The Empire Strikes Back",
                                               Cast = new[]
                                                          {
                                                              "Mark Hamill", 
                                                              "Harrison Ford", 
                                                              "Carrie Fisher"
                                                          }
                                           },
                                       new Movie
                                           {
                                               Title = "The Conversation",
                                               Cast =
                                                   new[]
                                                       {
                                                           "Gene Hackman", 
                                                           "John Cazale", 
                                                           "Allen Garfield", 
                                                           "Harrison Ford"
                                                       }
                                           },
                                       new Movie
                                           {
                                               Title = "Animal House",
                                               Cast = new[]
                                                          {
                                                              "John Belushi", 
                                                              "Karen Allen", 
                                                              "Tom Hulce"
                                                          }
                                           }
                                   };
		}

		public class Movie
		{
			public string Id { get; set; }
			public string Title { get; set; }
			public string[] Cast { get; set; }
		}

		public class MovieIndex : AbstractIndexCreationTask<Movie>
		{
			public MovieIndex()
			{
				Map = docs => from doc in docs
							  select new { doc.Cast };

				Analyzers = new Dictionary<Expression<Func<Movie, object>>, string>
                            {
                                {
                                    x => x.Cast,
                                    typeof (SimpleAnalyzer).FullName
                                    }
                            };

				Stores = new Dictionary<Expression<Func<Movie, object>>, FieldStorage>
                         {
                             {
                                 x => x.Cast, FieldStorage.Yes
                             }
                         };

				TermVector(x=>x.Cast, FieldTermVector.WithPositionsAndOffsets);
			}
		}

	}

	

}