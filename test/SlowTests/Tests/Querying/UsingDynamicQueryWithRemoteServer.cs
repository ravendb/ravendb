//-----------------------------------------------------------------------
// <copyright file="UsingDynamicQueryWithRemoteServer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;

using FastTests;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Operations.Databases.Indexes;
using Xunit;

namespace SlowTests.Tests.Querying
{
    public class UsingDynamicQueryWithRemoteServer : RavenNewTestBase
    {
        [Fact]
        public void CanPerformDynamicQueryUsingClientLinqQuery()
        {
            using (var documentStore = GetDocumentStore())
            {
                var blogOne = new Blog { Title = "one", Category = "Ravens" };
                var blogTwo = new Blog { Title = "two", Category = "Rhinos" };
                var blogThree = new Blog { Title = "three", Category = "Rhinos" };

                using (var s = documentStore.OpenSession())
                {
                    s.Store(blogOne);
                    s.Store(blogTwo);
                    s.Store(blogThree);
                    s.SaveChanges();
                }

                using (var s = documentStore.OpenSession())
                {
                    var results =
                        s.Query<Blog>()
                            .Customize(x => x.WaitForNonStaleResultsAsOfNow())
                            .Where(x => x.Category == "Rhinos" && x.Title.Length == 3)
                            .ToArray();

                    Assert.Equal(1, results.Length);
                    Assert.Equal("two", results[0].Title);
                    Assert.Equal("Rhinos", results[0].Category);
                }
            }
        }

        [Fact]
        public void CanPerformDynamicQueryUsingClientLuceneQuery()
        {
            using (var documentStore = GetDocumentStore())
            {
                var blogOne = new Blog { Title = "one", Category = "Ravens" };
                var blogTwo = new Blog { Title = "two", Category = "Rhinos" };
                var blogThree = new Blog { Title = "three", Category = "Rhinos" };

                using (var s = documentStore.OpenSession())
                {
                    s.Store(blogOne);
                    s.Store(blogTwo);
                    s.Store(blogThree);
                    s.SaveChanges();
                }

                using (var s = documentStore.OpenSession())
                {
                    var results =
                        s.Advanced.DocumentQuery<Blog>()
                            .Where("Title.Length:3 AND Category:Rhinos")
                            .WaitForNonStaleResultsAsOfNow()
                            .ToArray();

                    Assert.Equal(1, results.Length);
                    Assert.Equal("two", results[0].Title);
                    Assert.Equal("Rhinos", results[0].Category);
                }
            }
        }

        [Fact]
        public void CanPerformProjectionUsingClientLinqQuery()
        {
            using (var documentStore = GetDocumentStore())
            {
                var blogOne = new Blog
                {
                    Title = "one",
                    Category = "Ravens",
                    Tags = new[] { new Tag { Name = "tagOne" }, new Tag { Name = "tagTwo" } }
                };

                using (var s = documentStore.OpenSession())
                {
                    s.Store(blogOne);
                    s.SaveChanges();
                }

                using (var s = documentStore.OpenSession())
                {
                    var results =
                        s.Query<Blog>()
                            .Where(x => x.Title == "one" && x.Tags.Any(y => y.Name == "tagTwo"))
                            .Select(x => new { x.Category, x.Title })
                            .Single();

                    Assert.Equal("one", results.Title);
                    Assert.Equal("Ravens", results.Category);
                }
            }
        }

        [Fact]
        public void QueryForASpecificTypeDoesNotBringBackOtherTypes()
        {
            using (var documentStore = GetDocumentStore())
            {
                using (var s = documentStore.OpenSession())
                {
                    s.Store(new Tag());
                    s.SaveChanges();
                }

                using (var s = documentStore.OpenSession())
                {
                    var results = s.Query<Blog>().Select(b => new { b.Category }).ToArray();
                    Assert.Equal(0, results.Length);
                }
            }
        }

        [Fact]
        public void CanPerformLinqOrderByOnNumericField()
        {
            using (var documentStore = GetDocumentStore())
            {
                var blogOne = new Blog { SortWeight = 2 };

                var blogTwo = new Blog { SortWeight = 4 };

                var blogThree = new Blog { SortWeight = 1 };

                using (var s = documentStore.OpenSession())
                {
                    s.Store(blogOne);
                    s.Store(blogTwo);
                    s.Store(blogThree);
                    s.SaveChanges();
                }

                using (var s = documentStore.OpenSession())
                {
                    var resultDescending =
                        (from blog in s.Query<Blog>() orderby blog.SortWeight descending select blog).ToArray();

                    var resultAscending =
                        (from blog in s.Query<Blog>() orderby blog.SortWeight ascending select blog).ToArray();

                    Assert.Equal(4, resultDescending[0].SortWeight);
                    Assert.Equal(2, resultDescending[1].SortWeight);
                    Assert.Equal(1, resultDescending[2].SortWeight);

                    Assert.Equal(1, resultAscending[0].SortWeight);
                    Assert.Equal(2, resultAscending[1].SortWeight);
                    Assert.Equal(4, resultAscending[2].SortWeight);
                }
            }
        }

        [Fact]
        public void CanPerformLinqOrderByOnTextField()
        {
            using (var documentStore = GetDocumentStore())
            {
                var blogOne = new Blog { Title = "aaaaa" };

                var blogTwo = new Blog { Title = "ccccc" };

                var blogThree = new Blog { Title = "bbbbb" };

                using (var s = documentStore.OpenSession())
                {
                    s.Store(blogOne);
                    s.Store(blogTwo);
                    s.Store(blogThree);
                    s.SaveChanges();
                }

                using (var s = documentStore.OpenSession())
                {
                    var resultDescending =
                        (from blog in s.Query<Blog>() orderby blog.Title descending select blog).ToArray();

                    var resultAscending =
                        (from blog in s.Query<Blog>() orderby blog.Title ascending select blog).ToArray();

                    Assert.Equal("ccccc", resultDescending[0].Title);
                    Assert.Equal("bbbbb", resultDescending[1].Title);
                    Assert.Equal("aaaaa", resultDescending[2].Title);

                    Assert.Equal("aaaaa", resultAscending[0].Title);
                    Assert.Equal("bbbbb", resultAscending[1].Title);
                    Assert.Equal("ccccc", resultAscending[2].Title);
                }
            }
        }

        [Fact(Skip = "Missing feature: Highlighting")]
        public void CanPerformDynamicQueryWithHighlightingUsingClientLuceneQuery()
        {
            using (var documentStore = GetDocumentStore())
            {
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
                var blogThree = new Blog { Title = "Target cras vitae felis arcu word.", Category = "Los Rhinos" };

                string blogOneId;
                string blogTwoId;
                using (var s = documentStore.OpenSession())
                {
                    s.Store(blogOne);
                    s.Store(blogTwo);
                    s.Store(blogThree);
                    s.SaveChanges();

                    blogOneId = s.Advanced.GetDocumentId(blogOne);
                    blogTwoId = s.Advanced.GetDocumentId(blogTwo);
                }

                using (var s = documentStore.OpenSession())
                {
                    FieldHighlightings titleHighlightings;
                    FieldHighlightings categoryHighlightings;

                    var results =
                        s.Advanced.DocumentQuery<Blog>()
                            .Highlight("Title", 18, 2, out titleHighlightings)
                            .Highlight("Category", 18, 2, out categoryHighlightings)
                            .SetHighlighterTags("*", "*")
                            .Where("Title:(target word) OR Category:rhinos")
                            .WaitForNonStaleResultsAsOfNow()
                            .ToArray();

                    Assert.Equal(3, results.Length);
                    Assert.NotEmpty(titleHighlightings.GetFragments(blogOneId));
                    Assert.Empty(categoryHighlightings.GetFragments(blogOneId));

                    Assert.NotEmpty(titleHighlightings.GetFragments(blogTwoId));
                    Assert.NotEmpty(categoryHighlightings.GetFragments(blogTwoId));
                }
            }
        }

        [Fact(Skip = "Missing feature: Highlighting")]
        public void CanPerformDynamicQueryWithHighlighting()
        {
            using (var documentStore = GetDocumentStore())
            {
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
                var blogThree = new Blog { Title = "Target cras vitae felis arcu word.", Category = "Los Rhinos" };

                string blogOneId;
                string blogTwoId;
                using (var s = documentStore.OpenSession())
                {
                    s.Store(blogOne);
                    s.Store(blogTwo);
                    s.Store(blogThree);
                    s.SaveChanges();

                    blogOneId = s.Advanced.GetDocumentId(blogOne);
                    blogTwoId = s.Advanced.GetDocumentId(blogTwo);
                }

                using (var s = documentStore.OpenSession())
                {
                    FieldHighlightings titleHighlightings = null;
                    FieldHighlightings categoryHighlightings = null;

                    var results =
                        s.Query<Blog>()
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

        [Fact(Skip = "Missing feature: Highlighting")]
        public void ExecutesQueryWithHighlightingsAgainstSimpleIndex()
        {
            using (var documentStore = GetDocumentStore())
            {
                const string indexName = "BlogsForHighlightingTests";
                documentStore.Admin.Send(new PutIndexOperation(
                    indexName,
                    new IndexDefinition
                    {
                        Maps = { "from blog in docs.Blogs select new { blog.Title, blog.Category }" },
                        Fields = new Dictionary<string, IndexFieldOptions>
                        {
                            {"Title", new IndexFieldOptions { Storage = FieldStorage.Yes, Indexing = FieldIndexing.Analyzed, TermVector = FieldTermVector.WithPositionsAndOffsets} },
                            {"Category", new IndexFieldOptions { Storage = FieldStorage.Yes, Indexing = FieldIndexing.Analyzed, TermVector = FieldTermVector.WithPositionsAndOffsets} }
                        }
                    }));

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
                var blogThree = new Blog { Title = "Target cras vitae felis arcu word.", Category = "Los Rhinos" };

                string blogOneId;
                string blogTwoId;
                using (var s = documentStore.OpenSession())
                {
                    s.Store(blogOne);
                    s.Store(blogTwo);
                    s.Store(blogThree);
                    s.SaveChanges();

                    blogOneId = s.Advanced.GetDocumentId(blogOne);
                    blogTwoId = s.Advanced.GetDocumentId(blogTwo);
                }

                using (var s = documentStore.OpenSession())
                {
                    FieldHighlightings titleHighlightings = null;
                    FieldHighlightings categoryHighlightings = null;

                    var results =
                        s.Query<Blog>(indexName)
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

        [Fact(Skip = "Missing feature: Highlighting")]
        public void ExecutesQueryWithHighlightingsAndProjections()
        {
            using (var documentStore = GetDocumentStore())
            {
                const string indexName = "BlogsForHighlightingTests";
                documentStore.Admin.Send(new PutIndexOperation(
                    indexName,
                    new IndexDefinition
                    {
                        Maps = { "from blog in docs.Blogs select new { blog.Title, blog.Category }" },
                        Fields = new Dictionary<string, IndexFieldOptions>
                        {
                            {"Title", new IndexFieldOptions { Storage = FieldStorage.Yes, Indexing = FieldIndexing.Analyzed, TermVector = FieldTermVector.WithPositionsAndOffsets} },
                            {"Category", new IndexFieldOptions { Storage = FieldStorage.Yes, Indexing = FieldIndexing.Analyzed, TermVector = FieldTermVector.WithPositionsAndOffsets} }
                        }
                    }));

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
                var blogThree = new Blog { Title = "Target cras vitae felis arcu word.", Category = "Los Rhinos" };

                using (var s = documentStore.OpenSession())
                {
                    s.Store(blogOne);
                    s.Store(blogTwo);
                    s.Store(blogThree);
                    s.SaveChanges();
                }

                using (var s = documentStore.OpenSession())
                {
                    var results =
                        s.Query<Blog>(indexName)
                            .Customize(c => c.WaitForNonStaleResults().Highlight("Title", 18, 2, "TitleFragments"))
                            .Where(x => x.Title == "lorem" && x.Category == "ravens")
                            .Select(x => new { x.Title, x.Category, TitleFragments = default(string[]) })
                            .ToArray();

                    Assert.Equal(1, results.Length);
                    Assert.NotEmpty(results.First().TitleFragments);
                }
            }
        }

        [Fact(Skip = "Missing feature: Highlighting")]
        public void ExecutesQueryWithHighlightingsAgainstMapReduceIndex()
        {
            using (var documentStore = GetDocumentStore())
            {
                const string indexName = "BlogsForHighlightingMRTests";
                documentStore.Admin.Send(new PutIndexOperation(
                    indexName,
                    new IndexDefinition
                    {
                        Maps = { "from blog in docs.Blogs select new { blog.Title, blog.Category }" },
                        Reduce = @"from result in results 
                                   group result by result.Category into g
                                   select new { Category = g.Key, Title = g.Select(x=>x.Title).Aggregate(string.Concat) }",
                        Fields = new Dictionary<string, IndexFieldOptions>
                        {
                            {"Title", new IndexFieldOptions { Storage = FieldStorage.Yes, Indexing = FieldIndexing.Analyzed, TermVector = FieldTermVector.WithPositionsAndOffsets} },
                            {"Category", new IndexFieldOptions { Storage = FieldStorage.Yes, Indexing = FieldIndexing.Analyzed, TermVector = FieldTermVector.WithPositionsAndOffsets} }
                        }
                    }));

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
                var blogThree = new Blog { Title = "Target cras vitae felis arcu word.", Category = "Los Rhinos" };

                string blogOneId;
                string blogTwoId;
                using (var s = documentStore.OpenSession())
                {
                    s.Store(blogOne);
                    s.Store(blogTwo);
                    s.Store(blogThree);
                    s.SaveChanges();

                    blogOneId = s.Advanced.GetDocumentId(blogOne);
                    blogTwoId = s.Advanced.GetDocumentId(blogTwo);
                }

                using (var s = documentStore.OpenSession())
                {
                    var results =
                        s.Query<Blog>(indexName)
                            .Customize(c => c.WaitForNonStaleResults().Highlight("Title", 18, 2, "TitleFragments"))
                            .Where(x => x.Title == "lorem" && x.Category == "ravens")
                            .Select(x => new { x.Title, x.Category, TitleFragments = default(string[]) })
                            .ToArray();

                    Assert.Equal(1, results.Length);
                    Assert.NotEmpty(results.First().TitleFragments);
                }
            }
        }

        private class Blog
        {
            public User User { get; set; }

            public string Title { get; set; }

            public Tag[] Tags { get; set; }

            public int SortWeight { get; set; }

            public string Category { get; set; }
        }

        private class Tag
        {
            public string Name { get; set; }
        }

        private class User
        {
            public string Name { get; set; }
        }
    }
}
