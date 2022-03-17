using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using FastTests;
using Lucene.Net.Analysis;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class MoreLikeThisEvaluation : RavenTestBase
    {
        public MoreLikeThisEvaluation(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldMatchTwoMoviesWithSameCast()
        {
            using (var store = GetDocumentStore())
            {
                string id;
                using (var session = store.OpenSession())
                {
                    new MovieIndex().Execute(store);
                    var list = GetMovieList();
                    list.ForEach(session.Store);
                    session.SaveChanges();
                    id = session.Advanced.GetDocumentId(list.First());
                    Indexes.WaitForIndexing(store);
                }

                using (var session = store.OpenSession())
                {
                    var list = session
                        .Query<Movie, MovieIndex>()
                        .MoreLikeThis(f => f.UsingDocument(x => x.Id == id).WithOptions(new MoreLikeThisOptions
                        {
                            Fields = new[] { "Cast" },
                            MinimumTermFrequency = 1,
                            MinimumDocumentFrequency = 2
                        }))
                        .ToList();

                    Assert.NotEmpty(list);
                }
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

        private class Movie
        {
            public string Id { get; set; }
            public string Title { get; set; }
            public string[] Cast { get; set; }
        }

        private class MovieIndex : AbstractIndexCreationTask<Movie>
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

                TermVector(x => x.Cast, FieldTermVector.WithPositionsAndOffsets);
            }
        }

    }
}
