// -----------------------------------------------------------------------
//  <copyright file="Highlighting.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Xunit;

namespace Raven.Tests.Querying
{
    public class Highlighting : RavenTest
    {
        [Fact]
        public void ExecutesQueryWithHighlightingsAgainstSimpleIndex()
        {
            using (var store = this.NewDocumentStore())
            {
                const string indexName = "BlogsForHighlightingTests";
                store.DatabaseCommands.PutIndex(indexName,
                    new IndexDefinition
                    {
                        Map = "from blog in docs.Blogs select new { blog.Title, blog.Category }",
                        Stores =
                        {
                            {"Title", FieldStorage.Yes},
                            {"Category", FieldStorage.Yes}
                        },
                        Indexes =
                        {
                            {"Title", FieldIndexing.Analyzed},
                            {"Category", FieldIndexing.Analyzed}
                        }
                    });

                var blogOne = new Blog
                {
                    Title = "Lorem ipsum dolor sit amet, target word, consectetur adipiscing elit.",
                    Category = "Ravens"
                };
                var blogTwo = new Blog
                {
                    Title =
                        "Maecenas mauris leo, feugiat sodales facilisis target word, pellentesque, suscipit aliquet turpis.",
                    Category = "The Rhinos"
                };
                var blogThree = new Blog
                {
                    Title = "Target cras vitae felis arcu word.",
                    Category = "Los Rhinos"
                };

                string blogOneId;
                string blogTwoId;
                using (var s = store.OpenSession())
                {
                    s.Store(blogOne);
                    s.Store(blogTwo);
                    s.Store(blogThree);
                    s.SaveChanges();

                    blogOneId = s.Advanced.GetDocumentId(blogOne);
                    blogTwoId = s.Advanced.GetDocumentId(blogTwo);
                }

                using (var s = store.OpenSession())
                {
                    FieldHighlightings titleHighlightings = null;
                    FieldHighlightings categoryHighlightings = null;

                    var results = s.Query<Blog>(indexName)
                                   .Customize(
                                       c =>
                                       c.Highlight("Title", 18, 2, out titleHighlightings)
                                        .Highlight("Category", 18, 2, out categoryHighlightings)
                                        .SetHighlighterTags("*", "*")
                                        .WaitForNonStaleResultsAsOfNow())
                                   .Search(x => x.Category, "rhinos")
                                   .Search(x => x.Title, "target word")
                                   .ToArray();

                    Assert.Equal(3, results.Length);
                    Assert.NotEmpty(titleHighlightings.GetFragments(blogOneId));
                    Assert.Empty(categoryHighlightings.GetFragments(blogOneId));

                    Assert.NotEmpty(titleHighlightings.GetFragments(blogTwoId));
                    Assert.NotEmpty(categoryHighlightings.GetFragments(blogTwoId));
                }
            }
        }

        [Fact]
        public void ExecutesQueryWithHighlightingsAgainstMapReduceIndex()
        {
            using (var store = this.NewDocumentStore())
            {
                const string indexName = "BlogsForHighlightingMRTests";
                store.DatabaseCommands.PutIndex(indexName,
                    new IndexDefinition
                    {
                        Map = "from blog in docs.Blogs select new { blog.Title, blog.Category }",
                        Reduce = @"from result in results 
                                   group result by result.Category into g
                                   select new { Category = g.Key, Title = g.Select(x=>x.Title).Aggregate(string.Concat) }",
                        Stores =
                        {
                            {"Title", FieldStorage.Yes},
                            {"Category", FieldStorage.Yes}
                        },
                        Indexes =
                        {
                            {"Title", FieldIndexing.Analyzed},
                            {"Category", FieldIndexing.Analyzed}
                        }
                    });

                var blogOne = new Blog
                {
                    Title = "Lorem ipsum dolor sit amet, target word, consectetur adipiscing elit.",
                    Category = "Ravens"
                };
                var blogTwo = new Blog
                {
                    Title =
                        "Maecenas mauris leo, feugiat sodales facilisis target word, pellentesque, suscipit aliquet turpis.",
                    Category = "The Rhinos"
                };
                var blogThree = new Blog
                {
                    Title = "Target cras vitae felis arcu word.",
                    Category = "Los Rhinos"
                };

                string blogOneId;
                string blogTwoId;
                using (var s = store.OpenSession())
                {
                    s.Store(blogOne);
                    s.Store(blogTwo);
                    s.Store(blogThree);
                    s.SaveChanges();

                    blogOneId = s.Advanced.GetDocumentId(blogOne);
                    blogTwoId = s.Advanced.GetDocumentId(blogTwo);
                }

                using (var s = store.OpenSession())
                {
                    var results = s.Query<Blog>(indexName)
                                   .Customize(
                                       c => c.WaitForNonStaleResults().Highlight("Title", 18, 2, "TitleFragments"))
                                   .Where(x => x.Title == "lorem" && x.Category == "ravens")
                                   .Select(x => new
                                   {
                                       x.Title,
                                       x.Category,
                                       TitleFragments = default(string[])
                                   })
                                   .ToArray();

                    Assert.Equal(1, results.Length);
                    Assert.NotEmpty(results.First().TitleFragments);
                }
            }
        }

        [Fact]
        public void ExecutesQueryWithHighlightingsAndProjections()
        {
            using (var store = this.NewDocumentStore())
            {
                const string indexName = "BlogsForHighlightingTests";
                store.DatabaseCommands.PutIndex(indexName,
                    new IndexDefinition
                    {
                        Map = "from blog in docs.Blogs select new { blog.Title, blog.Category }",
                        Stores =
                        {
                            {"Title", FieldStorage.Yes},
                            {"Category", FieldStorage.Yes}
                        },
                        Indexes =
                        {
                            {"Title", FieldIndexing.Analyzed},
                            {"Category", FieldIndexing.Analyzed}
                        }
                    });

                var blogOne = new Blog
                {
                    Title = "Lorem ipsum dolor sit amet, target word, consectetur adipiscing elit.",
                    Category = "Ravens"
                };
                var blogTwo = new Blog
                {
                    Title =
                        "Maecenas mauris leo, feugiat sodales facilisis target word, pellentesque, suscipit aliquet turpis.",
                    Category = "The Rhinos"
                };
                var blogThree = new Blog
                {
                    Title = "Target cras vitae felis arcu word.",
                    Category = "Los Rhinos"
                };

                using (var s = store.OpenSession())
                {
                    s.Store(blogOne);
                    s.Store(blogTwo);
                    s.Store(blogThree);
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var results = s.Query<Blog>(indexName)
                                   .Customize(
                                       c => c.WaitForNonStaleResults().Highlight("Title", 18, 2, "TitleFragments"))
                                   .Where(x => x.Title == "lorem" && x.Category == "ravens")
                                   .Select(x => new
                                   {
                                       x.Title,
                                       x.Category,
                                       TitleFragments = default(string[])
                                   })
                                   .ToArray();

                    Assert.Equal(1, results.Length);
                    Assert.NotEmpty(results.First().TitleFragments);
                }
            }
        }



        public class Blog
        {
            public string Title { get; set; }
            public string Category { get; set; }
            public string[] TitleFragments { get; set; }
        }
    }
}