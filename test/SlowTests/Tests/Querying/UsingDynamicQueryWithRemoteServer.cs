//-----------------------------------------------------------------------
// <copyright file="UsingDynamicQueryWithRemoteServer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using Xunit.Abstractions;

using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries.Highlighting;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Tests.Querying
{
    public class UsingDynamicQueryWithRemoteServer : RavenTestBase
    {
        public UsingDynamicQueryWithRemoteServer(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanPerformDynamicQueryUsingClientLinqQuery(Options options)
        {
            using (var documentStore = GetDocumentStore(options))
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
                            .Customize(x => x.WaitForNonStaleResults())
                            .Where(x => x.Category == "Rhinos" && x.Title.Length == 3)
                            .ToArray();

                    Assert.Equal(1, results.Length);
                    Assert.Equal("two", results[0].Title);
                    Assert.Equal("Rhinos", results[0].Category);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanPerformDynamicQueryUsingClientLuceneQuery(Options options)
        {
            using (var documentStore = GetDocumentStore(options))
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
                            .WhereLucene("Title.Length", "3")
                            .AndAlso()
                            .WhereLucene("Category", "Rhinos")
                            .WaitForNonStaleResults()
                            .ToArray();

                    Assert.Equal(1, results.Length);
                    Assert.Equal("two", results[0].Title);
                    Assert.Equal("Rhinos", results[0].Category);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanPerformProjectionUsingClientLinqQuery(Options options)
        {
            using (var documentStore = GetDocumentStore(options))
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

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void QueryForASpecificTypeDoesNotBringBackOtherTypes(Options options)
        {
            using (var documentStore = GetDocumentStore(options))
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

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanPerformLinqOrderByOnNumericField(Options options)
        {
            using (var documentStore = GetDocumentStore(options))
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

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanPerformLinqOrderByOnTextField(Options options)
        {
            using (var documentStore = GetDocumentStore(options))
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

        [RavenTheory(RavenTestCategory.Highlighting)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanPerformDynamicQueryWithHighlightingUsingClientLuceneQuery(Options options)
        {
            using (var documentStore = GetDocumentStore(options))
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
                    var highlightingOptions = new HighlightingOptions
                    {
                        PreTags = new[] { "*" },
                        PostTags = new[] { "*" }
                    };

                    var results =
                        s.Advanced.DocumentQuery<Blog>()
                            .Highlight("Title", 18, 2, highlightingOptions, out Highlightings titleHighlightings)
                            .Highlight("Category", 18, 2, highlightingOptions, out Highlightings categoryHighlightings)
                            .Search("Title", "target word")
                            .Search("Category", "rhinos")
                            .WaitForNonStaleResults()
                            .ToArray();

                    Assert.Equal(3, results.Length);
                    Assert.NotEmpty(titleHighlightings.GetFragments(blogOneId));
                    Assert.Empty(categoryHighlightings.GetFragments(blogOneId));

                    Assert.NotEmpty(titleHighlightings.GetFragments(blogTwoId));
                    Assert.NotEmpty(categoryHighlightings.GetFragments(blogTwoId));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Highlighting)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanPerformDynamicQueryWithHighlighting(Options options)
        {
            using (var documentStore = GetDocumentStore(options))
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
                    var results = s.Query<Blog>()
                        .Customize(c => c.WaitForNonStaleResults())
                        .Highlight("Title", 18, 2, out Highlightings titleHighlightings)
                        .Highlight("Category", 18, 2, out Highlightings categoryHighlightings)
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

        [RavenTheory(RavenTestCategory.Highlighting)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void ExecutesQueryWithHighlightingsAgainstSimpleIndex(Options options)
        {
            using (var documentStore = GetDocumentStore(options))
            {
                const string indexName = "BlogsForHighlightingTests";
                documentStore.Maintenance.Send(new PutIndexesOperation(new[] {
                    new IndexDefinition
                    {
                        Name = indexName,
                        Maps = { "from blog in docs.Blogs select new { blog.Title, blog.Category }" },
                        Fields = new Dictionary<string, IndexFieldOptions>
                        {
                            {"Title", new IndexFieldOptions { Storage = FieldStorage.Yes, Indexing = FieldIndexing.Search, TermVector = FieldTermVector.WithPositionsAndOffsets} },
                            {"Category", new IndexFieldOptions { Storage = FieldStorage.Yes, Indexing = FieldIndexing.Search, TermVector = FieldTermVector.WithPositionsAndOffsets} }
                        }
                    }}));

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
                    var results = s.Query<Blog>(indexName)
                        .Customize(c => c.WaitForNonStaleResults())
                        .Highlight("Title", 18, 2, out Highlightings titleHighlightings)
                        .Highlight("Category", 18, 2, out Highlightings categoryHighlightings)
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

        [RavenTheory(RavenTestCategory.Highlighting)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void ExecutesQueryWithHighlightingsAndProjections(Options options)
        {
            using (var documentStore = GetDocumentStore(options))
            {
                const string indexName = "BlogsForHighlightingTests";
                documentStore.Maintenance.Send(new PutIndexesOperation(new[] {
                    new IndexDefinition
                    {
                        Name =indexName,
                        Maps = { "from blog in docs.Blogs select new { blog.Title, blog.Category }" },
                        Fields = new Dictionary<string, IndexFieldOptions>
                        {
                            {"Title", new IndexFieldOptions { Storage = FieldStorage.Yes, Indexing = FieldIndexing.Search, TermVector = FieldTermVector.WithPositionsAndOffsets} },
                            {"Category", new IndexFieldOptions { Storage = FieldStorage.Yes, Indexing = FieldIndexing.Search, TermVector = FieldTermVector.WithPositionsAndOffsets} }
                        }
                    }}));

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
                    var results = s.Query<Blog>(indexName)
                        .Customize(c => c.WaitForNonStaleResults())
                        .Highlight("Title", 18, 2, new HighlightingOptions
                        {
                            GroupKey = "Category"
                        }, out Highlightings highlightings)
                        .Where(x => x.Title == "lorem" && x.Category == "ravens")
                        .Select(x => new
                        {
                            x.Title,
                            x.Category
                        })
                        .ToArray();

                    Assert.Equal(1, results.Length);
                    Assert.NotEmpty(highlightings.GetFragments(results.First().Category));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Highlighting)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void ExecutesQueryWithHighlightingsAgainstMapReduceIndex(Options options)
        {
            using (var documentStore = GetDocumentStore(options))
            {
                const string indexName = "BlogsForHighlightingMRTests";
                documentStore.Maintenance.Send(new PutIndexesOperation(new[] {
                    new IndexDefinition
                    {
                        Name = indexName,
                        Maps = { "from blog in docs.Blogs select new { blog.Title, blog.Category }" },
                        Reduce = @"from result in results 
                                   group result by result.Category into g
                                   select new { Category = g.Key, Title = g.Select(x=>x.Title).Aggregate(string.Concat) }",
                        Fields = new Dictionary<string, IndexFieldOptions>
                        {
                            {"Title", new IndexFieldOptions { Storage = FieldStorage.Yes, Indexing = FieldIndexing.Search, TermVector = FieldTermVector.WithPositionsAndOffsets} },
                            {"Category", new IndexFieldOptions { Storage = FieldStorage.Yes, Indexing = FieldIndexing.Search, TermVector = FieldTermVector.WithPositionsAndOffsets} }
                        }
                    }}));

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
                    var results = s.Query<Blog>(indexName)
                        .Customize(c => c.WaitForNonStaleResults())
                        .Highlight("Title", 18, 2, new HighlightingOptions
                        {
                            GroupKey = "Category"
                        }, out Highlightings highlightings)
                        .Where(x => x.Title == "lorem" && x.Category == "ravens")
                        .Select(x => new
                        {
                            x.Title,
                            x.Category
                        })
                        .ToArray();

                    Assert.Equal(1, results.Length);
                    Assert.NotEmpty(highlightings.GetFragments(results.First().Category));
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
